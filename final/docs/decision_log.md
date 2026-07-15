# Apuntes de decisiones — Entrega Final Minería de Datos II

**Equipo:** Lucas Dugo, Cristian Lo Giudice, Rodolfo Berrone, Santiago Ham Saguier, Andrea Romeo.

## Cómo armamos el pipeline

- **Batch** para los 7 CSV; **streaming** para `usage_events_stream`.
- Keyspace Astra: **`default_keyspace`**.
- Cluster: **`db_final_istea`** (`c14f9098-…`); Secret Colab: **`ASTRA_TOKEN_FINAL`**.

## Bronze

- Esquemas **a mano** (`StructType`), sin `inferSchema`.
- `value` en eventos queda **String** en bronze; en silver lo pasamos a double.
- Sacamos duplicados por clave natural en cada CSV (`invoice_id`, `ticket_id`, etc.).
- Streaming: watermark **2 días**, dedupe por `event_id`, checkpoint en `final/checkpoints/bronze_stream`.
- `users`, `marketing_touches` y `nps_surveys` se ingestan en batch (pide el enunciado) pero **no** pasan a silver/gold: las 5 consultas del final no los usan.

## Silver

- Reglas **R1–R3** → quarantine.
- **schema_version** nulo → 1; `carbon_kg` / `genai_tokens` → coalesce 0.
- Flag **cost_spike** si `cost_usd_increment > p99 × 3` (para la tabla de anomalías).
- Join con `customers_orgs` y `resources`.
- Tickets: `ticket_date`, `is_critical` si severidad es high o critical.
- Billing: `revenue_usd = (subtotal - credits + taxes) * exchange_rate_to_usd`.

## Gold

- 5 resúmenes del enunciado, particionados por `usage_date` o `month`.
- Salida en `final/datalake/gold/`.

## Astra

- 5 tablas armadas para las consultas del enunciado.
- Carga: `foreachBatch` + `insert_many(..., chunk_size=10, ordered=True, concurrency=1)` con reintento si da `DataAPITimeoutException`.
- Antes de crear tabla: `drop_collection` / `drop_table` si ya existía.
- `tickets_by_org_date`: clustering con `add_partition_sort({"ticket_date": 1, "severity": 1})` — astrapy solo acepta 1/-1, no posición.
- Si un gold queda con 0 filas, no hay `.parquet` y `cargar_parquet_a_tabla` no carga (evita `UNABLE_TO_INFER_SCHEMA`).
- Checkpoints separados por tabla en `checkpoints/serving_*`.

## Re-ejecución en Colab

- Si el stream falla al correr de nuevo, limpiamos salida bronze stream y checkpoints con `rm -rf` al inicio.
