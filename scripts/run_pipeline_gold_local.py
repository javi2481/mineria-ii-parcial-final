"""Corre bronze->silver->gold local (sin Astra) para generar evidencias."""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "datalake" / "landing"
BASE = ROOT / "final"
DL = BASE / "datalake"


def main() -> None:
    spark = (
        SparkSession.builder.appName("final-gold-local")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    bronze_csv = DL / "bronze" / "batch"
    bronze_stream = DL / "bronze" / "stream" / "usage_events"
    ck = BASE / "checkpoints" / "bronze_stream"
    for p in [bronze_stream, ck]:
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)

    # --- bronze batch (resumido: mismos esquemas que notebook) ---
    def bronze_csv_table(csv_name: str, schema: StructType, dedupe_cols: list[str], part_col: str | None = None):
        out = bronze_csv / csv_name
        df = spark.read.option("header", True).schema(schema).csv(str(LANDING / f"{csv_name}.csv"))
        df = df.withColumn("ingest_ts", F.current_timestamp()).withColumn("source_file", F.input_file_name())
        df = df.dropDuplicates(dedupe_cols)
        w = df.write.mode("overwrite")
        (w.partitionBy(part_col) if part_col else w).parquet(str(out))
        return df

    bronze_csv_table(
        "customers_orgs",
        StructType([StructField(c, t) for c, t in [
            ("org_id", StringType()), ("org_name", StringType()), ("industry", StringType()),
            ("hq_region", StringType()), ("plan_tier", StringType()), ("is_enterprise", BooleanType()),
            ("signup_date", DateType()), ("sales_rep", StringType()), ("lifecycle_stage", StringType()),
            ("marketing_source", StringType()), ("nps_score", DoubleType()),
        ]]),
        ["org_id"], "hq_region",
    )
    bronze_csv_table(
        "users",
        StructType([StructField(c, t) for c, t in [
            ("user_id", StringType()), ("org_id", StringType()), ("email", StringType()),
            ("role", StringType()), ("active", BooleanType()), ("created_at", DateType()), ("last_login", DateType()),
        ]]),
        ["user_id"],
    )
    bronze_csv_table(
        "billing_monthly",
        StructType([StructField(c, t) for c, t in [
            ("invoice_id", StringType()), ("org_id", StringType()), ("month", DateType()),
            ("subtotal", DoubleType()), ("credits", DoubleType()), ("taxes", DoubleType()),
            ("currency", StringType()), ("exchange_rate_to_usd", DoubleType()),
        ]]),
        ["invoice_id"],
    )
    bronze_csv_table(
        "resources",
        StructType([StructField(c, t) for c, t in [
            ("resource_id", StringType()), ("org_id", StringType()), ("service", StringType()),
            ("region", StringType()), ("created_at", DateType()), ("state", StringType()), ("tags_json", StringType()),
        ]]),
        ["resource_id"],
    )
    bronze_csv_table(
        "support_tickets",
        StructType([StructField(c, t) for c, t in [
            ("ticket_id", StringType()), ("org_id", StringType()), ("category", StringType()),
            ("severity", StringType()), ("created_at", TimestampType()), ("resolved_at", TimestampType()),
            ("csat", DoubleType()), ("sla_breached", BooleanType()),
        ]]),
        ["ticket_id"],
    )
    bronze_csv_table(
        "marketing_touches",
        StructType([StructField(c, t) for c, t in [
            ("touch_id", StringType()), ("org_id", StringType()), ("campaign", StringType()),
            ("channel", StringType()), ("touch_date", DateType()), ("converted", BooleanType()),
        ]]),
        ["touch_id"],
    )
    bronze_csv_table(
        "nps_surveys",
        StructType([StructField(c, t) for c, t in [
            ("org_id", StringType()), ("survey_date", DateType()), ("nps_score", DoubleType()), ("comment", StringType()),
        ]]),
        ["org_id", "survey_date"],
    )

    esquema_eventos = StructType([
        StructField("event_id", StringType()), StructField("timestamp", TimestampType()),
        StructField("org_id", StringType()), StructField("resource_id", StringType()),
        StructField("service", StringType()), StructField("region", StringType()),
        StructField("metric", StringType()), StructField("value", StringType()),
        StructField("unit", StringType()), StructField("cost_usd_increment", DoubleType()),
        StructField("schema_version", IntegerType()), StructField("carbon_kg", DoubleType()),
        StructField("genai_tokens", LongType()),
    ])
    # En local leemos JSONL en batch (mismo resultado bronze que el stream del notebook)
    bronze_ev = (
        spark.read.schema(esquema_eventos).json(str(LANDING / "usage_events_stream"))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .dropDuplicates(["event_id"])
    )
    bronze_ev.write.mode("overwrite").parquet(str(bronze_stream))

    # --- silver ---
    eventos = spark.read.parquet(str(bronze_stream))
    eventos = eventos.withColumn("value_num", F.col("value").cast("double"))
    eventos = eventos.withColumn("usage_date", F.to_date("timestamp"))
    eventos = eventos.withColumn("schema_version", F.coalesce(F.col("schema_version"), F.lit(1)))
    eventos = eventos.withColumn("carbon_kg", F.coalesce(F.col("carbon_kg"), F.lit(0.0)))
    eventos = eventos.withColumn("genai_tokens", F.coalesce(F.col("genai_tokens"), F.lit(0)))

    r1 = F.col("event_id").isNull()
    r2 = F.col("cost_usd_increment") < -0.01
    r3 = F.col("value_num").isNotNull() & F.col("unit").isNull()
    p99 = eventos.approxQuantile("cost_usd_increment", [0.99], 0.01)[0]
    umbral = p99 * 3
    eventos = eventos.withColumn(
        "dq_rule",
        F.when(r1, "R1_event_id_nulo").when(r2, "R2_costo_anomalo").when(r3, "R3_unit_nulo_con_value"),
    ).withColumn("cost_spike_flag", F.col("cost_usd_increment") > umbral)
    inv = r1 | r2 | r3
    df_limpio = eventos.filter(~inv)
    eventos.filter(inv).write.mode("overwrite").parquet(str(DL / "silver" / "quarantine"))

    clientes = spark.read.parquet(str(bronze_csv / "customers_orgs")).select("org_id", "org_name", "plan_tier")
    recursos = spark.read.parquet(str(bronze_csv / "resources")).select("resource_id", "org_id")
    df_silver = df_limpio.join(clientes, "org_id", "left").join(recursos, ["resource_id", "org_id"], "left")
    df_silver = df_silver.withColumn("cpu_hours", F.when(F.col("metric") == "cpu_hours", F.col("value_num")).otherwise(0.0))
    df_silver = df_silver.withColumn("storage_gb_hours", F.when(F.col("metric") == "storage_gb_hours", F.col("value_num")).otherwise(0.0))
    cols = [
        "event_id", "timestamp", "usage_date", "org_id", "org_name", "plan_tier", "service", "region",
        "metric", "value_num", "unit", "cost_usd_increment", "schema_version", "carbon_kg", "genai_tokens",
        "cpu_hours", "storage_gb_hours", "cost_spike_flag",
    ]
    ruta_silver = DL / "silver" / "usage_events_clean"
    df_silver.select(*cols).write.mode("overwrite").partitionBy("usage_date").parquet(str(ruta_silver))

    tickets = spark.read.parquet(str(bronze_csv / "support_tickets"))
    tickets = tickets.withColumn("ticket_date", F.to_date("created_at")).withColumn(
        "is_critical", F.col("severity").isin("critical", "high")
    )
    tickets.write.mode("overwrite").partitionBy("ticket_date").parquet(str(DL / "silver" / "support_tickets_clean"))

    billing = spark.read.parquet(str(bronze_csv / "billing_monthly"))
    billing = billing.withColumn(
        "revenue_usd",
        (F.col("subtotal") - F.coalesce(F.col("credits"), F.lit(0.0)) + F.coalesce(F.col("taxes"), F.lit(0.0)))
        * F.col("exchange_rate_to_usd"),
    ).join(clientes, "org_id", "left")
    billing.write.mode("overwrite").partitionBy("month").parquet(str(DL / "silver" / "billing_monthly_clean"))

    # --- gold ---
    df_silver = spark.read.parquet(str(ruta_silver))
    gold = DL / "gold"
    df_silver.groupBy("org_id", "org_name", "plan_tier", "usage_date", "service").agg(
        F.round(F.sum("cost_usd_increment"), 4).alias("total_cost_usd"),
        F.sum(F.when(F.col("metric") == "requests", F.col("value_num")).otherwise(0)).cast("long").alias("total_requests"),
        F.round(F.sum("cpu_hours"), 4).alias("total_cpu_hours"),
        F.round(F.sum("storage_gb_hours"), 4).alias("total_storage_gb_hours"),
        F.sum("genai_tokens").cast("long").alias("total_genai_tokens"),
        F.round(F.sum("carbon_kg"), 6).alias("total_carbon_kg"),
        F.count("*").cast("int").alias("event_count"),
    ).write.mode("overwrite").partitionBy("usage_date").parquet(str(gold / "org_daily_usage_by_service"))

    billing = spark.read.parquet(str(DL / "silver" / "billing_monthly_clean"))
    billing.groupBy("org_id", "org_name", "plan_tier", "month").agg(
        F.round(F.sum("revenue_usd"), 2).alias("revenue_usd"),
        F.round(F.sum(F.coalesce(F.col("credits"), F.lit(0.0))), 2).alias("total_credits"),
        F.round(F.sum(F.coalesce(F.col("taxes"), F.lit(0.0))), 2).alias("total_taxes"),
        F.first("currency").alias("currency"),
    ).write.mode("overwrite").partitionBy("month").parquet(str(gold / "revenue_by_org_month"))

    df_silver.filter(F.col("cost_spike_flag")).groupBy("org_id", "org_name", "usage_date", "service").agg(
        F.count("*").cast("int").alias("spike_event_count"),
        F.round(F.max("cost_usd_increment"), 4).alias("max_cost_usd"),
        F.round(F.avg("cost_usd_increment"), 4).alias("avg_cost_usd"),
        F.lit("p99_x3").alias("anomaly_method"),
        F.lit(True).alias("is_anomaly"),
    ).write.mode("overwrite").partitionBy("usage_date").parquet(str(gold / "cost_anomaly_mart"))

    tickets = spark.read.parquet(str(DL / "silver" / "support_tickets_clean"))
    tickets.groupBy("org_id", "ticket_date", "severity").agg(
        F.count("*").cast("int").alias("ticket_count"),
        F.sum(F.when(F.col("is_critical"), 1).otherwise(0)).cast("int").alias("critical_count"),
        F.round(F.avg(F.when(F.col("sla_breached"), 1.0).otherwise(0.0)), 4).alias("sla_breach_rate"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
    ).write.mode("overwrite").partitionBy("ticket_date").parquet(str(gold / "tickets_by_org_date"))

    genai = df_silver.filter(F.col("service") == "genai")
    genai.groupBy("org_id", "org_name", "usage_date").agg(
        F.sum("genai_tokens").cast("long").alias("total_tokens"),
        F.round(F.sum("cost_usd_increment"), 4).alias("estimated_cost_usd"),
    ).write.mode("overwrite").partitionBy("usage_date").parquet(str(gold / "genai_tokens_by_org_date"))

    print("gold listo en", gold)
    spark.stop()


if __name__ == "__main__":
    main()
