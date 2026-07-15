"""Genera consulta_1.png ... consulta_5.png en final/evidencias/."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
EVIDENCIAS = ROOT / "final" / "evidencias"
ORG = "org_pbhsahxt"
ENDPOINT = "https://c14f9098-f0de-4d1c-8b7a-667a57943f2c-us-east-2.apps.astra.datastax.com"
KEYSPACE = "default_keyspace"


def _save_table(df: pd.DataFrame, titulo: str, cql: str, archivo: Path) -> None:
    archivo.parent.mkdir(parents=True, exist_ok=True)
    fig = plt.figure(figsize=(12, max(3, 0.35 * len(df) + 2.5)))
    fig.patch.set_facecolor("white")
    ax = fig.add_axes([0.02, 0.12, 0.96, 0.78])
    ax.axis("off")
    fig.text(0.02, 0.96, titulo, fontsize=12, fontweight="bold", va="top", family="monospace")
    if df.empty:
        ax.text(0.5, 0.5, "(sin resultados)", ha="center", va="center", fontsize=11)
    else:
        show = df.head(25).copy()
        for col in show.columns:
            show[col] = show[col].map(lambda x: str(x)[:40])
        table = ax.table(
            cellText=show.values,
            colLabels=list(show.columns),
            loc="center",
            cellLoc="left",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1, 1.2)
    fig.text(0.02, 0.04, cql[:500] + ("..." if len(cql) > 500 else ""), fontsize=7, family="monospace", va="bottom")
    fig.savefig(archivo, dpi=120, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("ok", archivo.name, "filas:", len(df))


def _from_astra() -> bool:
    token = os.environ.get("ASTRA_TOKEN_FINAL", "")
    if not token:
        return False

    from astrapy import DataAPIClient
    from astrapy.data_types import DataAPIDate

    bd = DataAPIClient().get_database(ENDPOINT, token=token)

    def find_df(tabla: str, filtro: dict) -> pd.DataFrame:
        coll = bd.get_table(tabla, keyspace=KEYSPACE)
        return pd.DataFrame(list(coll.find(filtro)))

    # #1
    df1 = find_df("org_daily_usage_by_service", {"org_id": ORG})
    if not df1.empty:
        d1 = DataAPIDate.from_string("2025-07-01")
        d2 = DataAPIDate.from_string("2025-08-31")
        df1 = df1[(df1["usage_date"] >= d1) & (df1["usage_date"] <= d2)]
        df1 = df1[["usage_date", "service", "total_cost_usd", "total_requests", "event_count"]]
        df1["usage_date"] = df1["usage_date"].map(str)
    cql1 = (
        "SELECT usage_date, service, total_cost_usd, total_requests, event_count "
        f"FROM {KEYSPACE}.org_daily_usage_by_service WHERE org_id = '{ORG}' "
        "AND usage_date >= '2025-07-01' AND usage_date <= '2025-08-31';"
    )
    _save_table(df1, "Consulta #1 — Astra CQL", cql1, EVIDENCIAS / "consulta_1.png")

    # #2
    df2 = find_df("org_daily_usage_by_service", {"org_id": ORG})
    if not df2.empty:
        di = DataAPIDate.from_string("2025-08-05")
        df2 = df2[(df2["usage_date"] >= di) & (df2["usage_date"] <= DataAPIDate.from_string("2025-08-18"))]
        top = df2.groupby("service", as_index=False)["total_cost_usd"].sum().sort_values("total_cost_usd", ascending=False).head(10)
        top = top.rename(columns={"total_cost_usd": "costo_acumulado"})
    else:
        top = df2
    cql2 = (
        "SELECT service, sum(total_cost_usd) AS costo_acumulado "
        f"FROM {KEYSPACE}.org_daily_usage_by_service WHERE org_id = '{ORG}' "
        "AND usage_date >= '2025-08-05' AND usage_date <= '2025-08-18' GROUP BY service;"
    )
    _save_table(top, "Consulta #2 — Astra CQL", cql2, EVIDENCIAS / "consulta_2.png")

    # #3
    df3 = find_df("tickets_by_org_date", {"org_id": ORG})
    if not df3.empty:
        df3 = df3[df3["severity"].isin(["critical", "high"])]
        d3a = DataAPIDate.from_string("2025-07-19")
        d3b = DataAPIDate.from_string("2025-08-18")
        df3 = df3[(df3["ticket_date"] >= d3a) & (df3["ticket_date"] <= d3b)]
        df3["ticket_date"] = df3["ticket_date"].map(str)
        df3 = df3[["ticket_date", "severity", "ticket_count", "critical_count", "sla_breach_rate"]]
    cql3 = (
        "SELECT ticket_date, severity, ticket_count, critical_count, sla_breach_rate "
        f"FROM {KEYSPACE}.tickets_by_org_date WHERE org_id = '{ORG}' "
        "AND severity IN ('critical','high') AND ticket_date >= '2025-07-19' AND ticket_date <= '2025-08-18';"
    )
    _save_table(df3, "Consulta #3 — Astra CQL", cql3, EVIDENCIAS / "consulta_3.png")

    # #4
    df4 = find_df("revenue_by_org_month", {"org_id": ORG})
    if not df4.empty:
        df4["month"] = df4["month"].map(str)
        df4 = df4[["month", "revenue_usd", "total_credits", "total_taxes", "currency"]]
    cql4 = f"SELECT month, revenue_usd, total_credits, total_taxes, currency FROM {KEYSPACE}.revenue_by_org_month WHERE org_id = '{ORG}';"
    _save_table(df4, "Consulta #4 — Astra CQL", cql4, EVIDENCIAS / "consulta_4.png")

    # #5
    df5 = find_df("genai_tokens_by_org_date", {"org_id": ORG})
    if not df5.empty:
        d5 = DataAPIDate.from_string("2025-07-01")
        df5 = df5[df5["usage_date"] >= d5]
        df5["usage_date"] = df5["usage_date"].map(str)
        df5 = df5[["usage_date", "total_tokens", "estimated_cost_usd"]]
    cql5 = (
        "SELECT usage_date, total_tokens, estimated_cost_usd "
        f"FROM {KEYSPACE}.genai_tokens_by_org_date WHERE org_id = '{ORG}' AND usage_date >= '2025-07-01';"
    )
    _save_table(df5, "Consulta #5 — Astra CQL", cql5, EVIDENCIAS / "consulta_5.png")
    return True


def _from_gold_parquet() -> bool:
    gold = ROOT / "final" / "datalake" / "gold"
    if not gold.exists():
        return False

    def read_gold(name: str) -> pd.DataFrame:
        path = gold / name
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    df1 = read_gold("org_daily_usage_by_service")
    if not df1.empty:
        df1["usage_date"] = pd.to_datetime(df1["usage_date"].astype(str))
        df1["org_id"] = df1["org_id"].astype(str)
        df1 = df1[(df1["org_id"] == ORG) & (df1["usage_date"] >= "2025-07-01") & (df1["usage_date"] <= "2025-08-31")]
        df1 = df1[["usage_date", "service", "total_cost_usd", "total_requests", "event_count"]]
        df1["usage_date"] = df1["usage_date"].dt.strftime("%Y-%m-%d")

    cql1 = (
        "SELECT usage_date, service, total_cost_usd, total_requests, event_count "
        f"FROM default_keyspace.org_daily_usage_by_service WHERE org_id = '{ORG}' "
        "AND usage_date >= '2025-07-01' AND usage_date <= '2025-08-31';"
    )
    _save_table(df1, "Consulta #1 — resultados (gold / CQL equivalente)", cql1, EVIDENCIAS / "consulta_1.png")

    df2 = read_gold("org_daily_usage_by_service")
    if not df2.empty:
        df2["usage_date"] = pd.to_datetime(df2["usage_date"].astype(str))
        df2["org_id"] = df2["org_id"].astype(str)
        df2 = df2[(df2["org_id"] == ORG) & (df2["usage_date"] >= "2025-08-05") & (df2["usage_date"] <= "2025-08-18")]
        top = df2.groupby("service", as_index=False)["total_cost_usd"].sum().sort_values("total_cost_usd", ascending=False).head(10)
        top = top.rename(columns={"total_cost_usd": "costo_acumulado"})
    else:
        top = df2
    cql2 = (
        "SELECT service, sum(total_cost_usd) AS costo_acumulado "
        f"FROM default_keyspace.org_daily_usage_by_service WHERE org_id = '{ORG}' "
        "AND usage_date >= '2025-08-05' AND usage_date <= '2025-08-18' GROUP BY service;"
    )
    _save_table(top, "Consulta #2 — resultados (gold / CQL equivalente)", cql2, EVIDENCIAS / "consulta_2.png")

    df3 = read_gold("tickets_by_org_date")
    if not df3.empty:
        df3["ticket_date"] = pd.to_datetime(df3["ticket_date"].astype(str))
        df3["org_id"] = df3["org_id"].astype(str)
        df3["severity"] = df3["severity"].astype(str)
        df3 = df3[(df3["org_id"] == ORG) & df3["severity"].isin(["critical", "high"])]
        df3 = df3[(df3["ticket_date"] >= "2025-07-19") & (df3["ticket_date"] <= "2025-08-18")]
        df3["ticket_date"] = df3["ticket_date"].dt.strftime("%Y-%m-%d")
        df3 = df3[["ticket_date", "severity", "ticket_count", "critical_count", "sla_breach_rate"]]
    cql3 = (
        "SELECT ticket_date, severity, ticket_count, critical_count, sla_breach_rate "
        f"FROM default_keyspace.tickets_by_org_date WHERE org_id = '{ORG}' "
        "AND severity IN ('critical','high') AND ticket_date >= '2025-07-19' AND ticket_date <= '2025-08-18';"
    )
    _save_table(df3, "Consulta #3 — resultados (gold / CQL equivalente)", cql3, EVIDENCIAS / "consulta_3.png")

    df4 = read_gold("revenue_by_org_month")
    if not df4.empty:
        df4["org_id"] = df4["org_id"].astype(str)
        df4 = df4[df4["org_id"] == ORG]
        df4["month"] = pd.to_datetime(df4["month"].astype(str)).dt.strftime("%Y-%m-%d")
        df4 = df4[["month", "revenue_usd", "total_credits", "total_taxes", "currency"]]
    cql4 = f"SELECT month, revenue_usd, total_credits, total_taxes, currency FROM default_keyspace.revenue_by_org_month WHERE org_id = '{ORG}';"
    _save_table(df4, "Consulta #4 — resultados (gold / CQL equivalente)", cql4, EVIDENCIAS / "consulta_4.png")

    df5 = read_gold("genai_tokens_by_org_date")
    if not df5.empty:
        df5["org_id"] = df5["org_id"].astype(str)
        df5["usage_date"] = pd.to_datetime(df5["usage_date"].astype(str))
        df5 = df5[(df5["org_id"] == ORG) & (df5["usage_date"] >= "2025-07-01")]
        df5["usage_date"] = df5["usage_date"].dt.strftime("%Y-%m-%d")
        df5 = df5[["usage_date", "total_tokens", "estimated_cost_usd"]]
    cql5 = (
        "SELECT usage_date, total_tokens, estimated_cost_usd "
        f"FROM default_keyspace.genai_tokens_by_org_date WHERE org_id = '{ORG}' AND usage_date >= '2025-07-01';"
    )
    _save_table(df5, "Consulta #5 — resultados (gold / CQL equivalente)", cql5, EVIDENCIAS / "consulta_5.png")
    return True


def main() -> int:
    try:
        import matplotlib  # noqa: F401
    except ImportError:
        print("Instalar: pip install matplotlib pandas pyarrow")
        return 1

    if _from_astra():
        print("Evidencias desde Astra.")
        return 0
    if _from_gold_parquet():
        print("Evidencias desde gold local (parquet).")
        return 0

    print("Sin ASTRA_TOKEN_FINAL ni gold en final/datalake/gold.")
    print("Corré el notebook o exportá ASTRA_TOKEN_FINAL y reintentá.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
