# Landing — datos crudos

Zona de entrada del data lake. **No se modifica** en los notebooks; solo se lee desde acá.

## Archivos batch (CSV)

| Archivo | Contenido |
|---|---|
| `customers_orgs.csv` | Organizaciones / clientes |
| `users.csv` | Usuarios por org |
| `resources.csv` | Recursos cloud por org |
| `support_tickets.csv` | Tickets de soporte |
| `marketing_touches.csv` | Contactos de marketing |
| `nps_surveys.csv` | Encuestas NPS |
| `billing_monthly.csv` | Facturación mensual |

## Streaming (JSONL)

`usage_events_stream/*.jsonl` — eventos de uso en tiempo casi real.

- A partir de **2025-07-18** cambia el esquema (`schema_version=2`): aparecen `genai_tokens` y `carbon_kg`.
- Pensado para Structured Streaming (watermark, checkpoint en el notebook).

## Notas del dataset

- ~60 días de eventos.
- Hay NULLs, tipos inconsistentes (ej. `value` a veces como string), costos negativos y outliers a propósito.
- Facturación mensual: meses `2025-06-01`, `2025-07-01`, `2025-08-01`.

Los notebooks leen desde `datalake/landing/` y escriben bronze/silver/gold en carpetas de salida (no van al repo).
