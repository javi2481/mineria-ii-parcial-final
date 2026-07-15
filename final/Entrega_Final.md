# Entrega Final — Minería de Datos II
**Minería de Datos II · ISTEA · Primer Cuatrimestre 2026**

## Equipo

- Lucas Dugo
- Cristian Lo Giudice
- Rodolfo Berrone
- Santiago Ham Saguier
- Andrea Romeo

**Repo:** https://github.com/javi2481/mineria-ii-parcial-final  
**Notebook:** `final/final_colab.ipynb` (PySpark en Google Colab)  
**Astra:** cluster `db_final_istea`, keyspace `default_keyspace`, secret `ASTRA_TOKEN_FINAL`

---

## 1. Arquitectura final

Hicimos un pipeline con **batch + streaming** y al final subimos tablas a Astra:

```
                 BATCH (7 CSV)
 datalake/landing  ──────────────►  bronze  ──►  silver  ──►  gold  ──►  AstraDB
                 STREAMING (JSONL)      │           │
                                        └─ quarantine
```

**Por qué batch + streaming (y no todo stream):** los CSV maestros y la facturación cambian poco; solo los eventos JSONL necesitan streaming. Así cumplimos el enunciado sin complicar el CRM ni el billing como si fueran tiempo real.

**Tecnologías:** PySpark en Colab, Parquet intermedio, Astra con `astrapy` y carga con `foreachBatch`.

---

## 2. Zonas del Data Lake

| Zona | Dónde | Qué hace |
|---|---|---|
| **Landing** | `datalake/landing/` (repo raíz) | Datos crudos, no los tocamos |
| **Bronze** | `final/datalake/bronze/` | Mismo grano que la fuente, esquema a mano, dedupe, `ingest_ts` y `source_file` |
| **Silver** | `final/datalake/silver/` | Limpieza, reglas R1–R3, joins, features |
| **Quarantine** | `final/datalake/silver/quarantine/` | Filas malas con columna `dq_rule` |
| **Gold** | `final/datalake/gold/` | 5 resúmenes listos para Astra |

En bronze dejamos `value` como **String**; en silver lo pasamos a `value_num` (double).

---

## 3. Ingesta

### 3.1 Batch (7 CSV → Bronze)

Leemos desde `datalake/landing/` con `StructType` por archivo:

- `customers_orgs`, `users`, `resources`, `support_tickets`, `marketing_touches`, `nps_surveys`, `billing_monthly`
- Dedupe por clave natural (`org_id`, `user_id`, `ticket_id`, `invoice_id`, etc.)
- Guardamos en `final/datalake/bronze/batch/`

`users`, `marketing_touches` y `nps_surveys` quedan en bronze (el enunciado pide ingestarlos en batch); no entran a los 5 gold del final porque las consultas no los usan.

### 3.2 Streaming (JSONL → Bronze)

- Esquema explícito de 13 campos (incluye `schema_version`, `carbon_kg`, `genai_tokens`)
- Watermark **2 días**, dedupe por `event_id`
- Checkpoint en `final/checkpoints/bronze_stream`
- Salida en `final/datalake/bronze/stream/usage_events/`
- `maxFilesPerTrigger=10` y `trigger(availableNow=True)` para correr en Colab

---

## 4. Calidad de datos y quarantine

| Regla | Rechaza si… | `dq_rule` |
|---|---|---|
| **R1** | `event_id` nulo | `R1_event_id_nulo` |
| **R2** | `cost_usd_increment < -0.01` | `R2_costo_anomalo` |
| **R3** | hay `value_num` pero `unit` es nulo | `R3_unit_nulo_con_value` |

- Lo malo va a quarantine; no borramos nada.
- **Picos de costo:** `cost_spike_flag` si `cost > p99 × 3` (método p-tiles). No va a quarantine, solo marca para `cost_anomaly_mart`.
- **Schema v1/v2:** `schema_version` nulo → 1; `carbon_kg` y `genai_tokens` → 0 si vienen nulos.

El notebook imprime conteos de quarantine, p99 y umbral.

---

## 5. Silver y Gold con features

### 5.1 Silver

- Join de eventos con `customers_orgs` y `resources` (`org_name`, `plan_tier`)
- Features: `usage_date`, `value_num`, `cpu_hours`, `storage_gb_hours`, `cost_spike_flag`, `carbon_kg`, `genai_tokens`
- Tickets: `ticket_date`, `is_critical` si severidad es high/critical
- Billing: `revenue_usd = (subtotal - credits + taxes) * exchange_rate_to_usd`

### 5.2 Gold — 5 resúmenes

| Tabla | Grano | Métricas principales |
|---|---|---|
| `org_daily_usage_by_service` | org × día × servicio | costos, requests, cpu, storage, genai, carbono |
| `revenue_by_org_month` | org × mes | revenue USD, créditos, impuestos |
| `cost_anomaly_mart` | org × día × servicio | picos p99×3 (puede quedar vacío) |
| `tickets_by_org_date` | org × día × severidad | counts, SLA, CSAT |
| `genai_tokens_by_org_date` | org × día | tokens y costo estimado |

Salida en `final/datalake/gold/`, particionado por `usage_date` o `month`.

---

## 6. Tablas en Cassandra (Astra)

Armamos las tablas pensando en las 5 consultas del enunciado. CQL de referencia en `final/cql/01_create_keyspace_table.cql`; en Colab las creamos con `astrapy`.

| Tabla | Partition key | Clustering |
|---|---|---|
| `org_daily_usage_by_service` | `(org_id, usage_date)` | `service` |
| `revenue_by_org_month` | `org_id` | `month` |
| `cost_anomaly_mart` | `(org_id, usage_date)` | `service` |
| `tickets_by_org_date` | `org_id` | `ticket_date`, `severity` |
| `genai_tokens_by_org_date` | `org_id` | `usage_date` |

Las consultas por `org_id` (y rango de fecha donde aplica) pegan en pocas particiones.

**Detalles astrapy** (más en `final/docs/decision_log.md`):

- Borramos tabla/collection vieja antes de crear
- `add_partition_sort` solo acepta `1`/`-1`, no posición
- Carga en lotes de 10 con reintento si hay timeout
- Si un gold queda vacío, no cargamos (evita error de schema)

Checkpoints de carga en `final/checkpoints/serving_*`.

---

## 7. Idempotencia

1. **Checkpoints** en bronze stream y en cada carga a Astra
2. **Dedupe** por `event_id` en stream y por clave natural en CSV
3. **Upsert** en Cassandra: misma PK no duplica

Prueba en el notebook: corremos el stream otra vez y el conteo en bronze no cambia (`antes == despues`).

---

## 8. Performance

- Particionamos silver/gold por `usage_date`, `month` o `ticket_date`
- `maxFilesPerTrigger=10` para no saturar memoria en Colab
- El notebook muestra MB por zona (bronze, silver, gold)
- Insertamos en Astra de a 10 filas para evitar timeout (más lento pero estable)

---

## 9. Seguridad y gobierno

- Token en secret `ASTRA_TOKEN_FINAL`, no en el repo
- Landing no se modifica
- `ingest_ts` + `source_file` en bronze para linaje
- Quarantine auditable con `dq_rule`
- Diccionario: secciones 5 y 6 de este doc
- Apuntes: `final/docs/decision_log.md`

---

## 10. Testing

- Idempotencia del stream (sección 7)
- Conteos por capa en el notebook
- R1–R3 con filas en quarantine impresas
- Las 5 consultas como prueba end-to-end
- `cost_anomaly_mart` vacío: la carga se omite sin romper

---

## 11. Consultas mínimas (CQL + capturas)

CQL completo en `final/cql/02_queries.cql`. Org de ejemplo: `org_pbhsahxt`. Capturas en `final/evidencias/consulta_1.png` … `consulta_5.png`.

**#1 — Costos y requests diarios por org y servicio**
```sql
SELECT usage_date, service, total_cost_usd, total_requests, event_count
FROM default_keyspace.org_daily_usage_by_service
WHERE org_id = 'org_pbhsahxt'
  AND usage_date >= '2025-07-01' AND usage_date <= '2025-08-31';
```

**#2 — Top servicios por costo (~14 días)**
```sql
SELECT service, sum(total_cost_usd) AS costo_acumulado
FROM default_keyspace.org_daily_usage_by_service
WHERE org_id = 'org_pbhsahxt'
  AND usage_date >= '2025-08-05' AND usage_date <= '2025-08-18'
GROUP BY service;
```
El top-N ordenado lo hace el notebook en pandas (como haría un BI).

**#3 — Tickets críticos/altos y SLA (últimos 30 días)**
```sql
SELECT ticket_date, severity, ticket_count, critical_count, sla_breach_rate
FROM default_keyspace.tickets_by_org_date
WHERE org_id = 'org_pbhsahxt'
  AND severity IN ('critical', 'high')
  AND ticket_date >= '2025-07-19' AND ticket_date <= '2025-08-18';
```

**#4 — Revenue mensual en USD**
```sql
SELECT month, revenue_usd, total_credits, total_taxes, currency
FROM default_keyspace.revenue_by_org_month
WHERE org_id = 'org_pbhsahxt';
```

**#5 — Tokens GenAI por día**
```sql
SELECT usage_date, total_tokens, estimated_cost_usd
FROM default_keyspace.genai_tokens_by_org_date
WHERE org_id = 'org_pbhsahxt'
  AND usage_date >= '2025-07-01';
```

---

## 12. Guía de uso

1. Abrí `final/final_colab.ipynb` en Colab.
2. Cargá el secret **`ASTRA_TOKEN_FINAL`** (token de `db_final_istea`).
3. **Ejecutar todo:** clona el repo, arma bronze/silver/gold en `final/datalake/`, carga Astra, corre consultas e idempotencia.
4. Para capturas CQL: Astra → CQL Console → pegar queries de `final/cql/02_queries.cql`.
5. Si el stream falla al re-correr, el notebook limpia checkpoint y salida bronze (ver decision_log).

---

## 13. Checklist de entrega

- [x] Código reproducible (notebook) landing → bronze → silver → gold → Astra
- [x] Reglas R1–R3 + quarantine con `dq_rule`
- [x] 5 resúmenes gold + CQL de creación y carga
- [x] 5 consultas mínimas (`final/cql/02_queries.cql` + notebook)
- [x] Capturas de las 5 consultas en `final/evidencias/`
- [x] Diagrama de arquitectura (sección 1 + presentación)
- [x] Diccionario de datos (secciones 5 y 6)
- [x] Log de decisiones (`final/docs/decision_log.md`)
- [x] README del repo
- [x] Presentación (`Presentacion_Final.pptx`)
- [x] Video ≤ 20 min (`Final_Mineria.mp4`)
