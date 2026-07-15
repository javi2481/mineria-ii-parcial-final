# Minería de Datos II

Repo con el **segundo parcial** y el **examen final**.
Los datos van de landing a bronze, silver y gold; al final cargamos tablas en Astra (`default_keyspace`).

Repo: [github.com/javi2481/mineria-ii-parcial-final](https://github.com/javi2481/mineria-ii-parcial-final)

## Estructura

```
mineria-ii-parcial-final/
├── README.md
├── datalake/landing/          # datos crudos compartidos (7 CSV + JSONL)
├── scripts/                   # utilidades locales (evidencias, pipeline gold)
├── parcial/
│   ├── parcial2_colab.ipynb
│   ├── cql/
│   └── docs/decision_log.md
└── final/
    ├── final_colab.ipynb
    ├── Entrega_Final.md
    ├── Presentacion_Final.pptx
    ├── Final_Mineria.mp4
    ├── datalake/              # salida del FINAL (generada al correr; en .gitignore)
    ├── cql/
    ├── docs/decision_log.md
    └── evidencias/
```

---

## Bases Astra (distintas)

| Examen | Base | Secret Colab |
|--------|------|--------------|
| Parcial | `db_rodolfo_istea` | `ASTRA_TOKEN` |
| Final | `db_final_istea` | `ASTRA_TOKEN_FINAL` |

En Colab hay que cargar el Application Token del cluster que toque en cada secret.

---

## Segundo Parcial

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/javi2481/mineria-ii-parcial-final/blob/main/parcial/parcial2_colab.ipynb)

**Notebook:** [`parcial/parcial2_colab.ipynb`](parcial/parcial2_colab.ipynb)

### Qué hicimos

1. Bronze batch — 3 CSV con esquema a mano
2. Bronze streaming — watermark y checkpoint
3. Silver — 3 reglas; lo malo va a quarantine
4. Gold — resumen diario por org, día y servicio
5. Astra — subimos con `foreachBatch` y probamos 2 consultas
6. Idempotencia del stream

### Cómo correr

1. Abrís el notebook en Colab (badge de arriba)
2. **Entorno de ejecución → Ejecutar todo**
3. Secret Colab: **`ASTRA_TOKEN`** (cluster `db_rodolfo_istea`)

### Astra

- Tabla: `default_keyspace.examen_mineria_II`
- CQL: [`parcial/cql/02_queries.cql`](parcial/cql/02_queries.cql)

---

## Examen Final

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/javi2481/mineria-ii-parcial-final/blob/main/final/final_colab.ipynb)

**Notebook:** [`final/final_colab.ipynb`](final/final_colab.ipynb)

### Qué hicimos

1. Bronze batch — 7 CSV con esquema a mano
2. Bronze streaming — watermark y checkpoint
3. Silver — R1, R2 y R3 + quarantine; marcamos costos altos con p99×3
4. Gold — 5 resúmenes (uso, facturación, anomalías, tickets, tokens GenAI)
5. Astra — 5 tablas, carga con `foreachBatch` y 5 consultas
6. Idempotencia del stream

### Cómo correr

1. Abrís el notebook en Colab (badge de arriba)
2. **Entorno de ejecución → Ejecutar todo**
3. Secret Colab: **`ASTRA_TOKEN_FINAL`** (cluster `db_final_istea`)

### Tablas Astra (`default_keyspace`)

| Tabla | Qué muestra |
|-------|-------------|
| `org_daily_usage_by_service` | Uso diario por org y servicio |
| `revenue_by_org_month` | Facturación mensual por org |
| `cost_anomaly_mart` | Picos de costo (p99×3) |
| `tickets_by_org_date` | Tickets de soporte por día |
| `genai_tokens_by_org_date` | Tokens GenAI por día |

- CQL: [`final/cql/02_queries.cql`](final/cql/02_queries.cql)
- Capturas: carpeta [`final/evidencias/`](final/evidencias/)
- Entrega: [`final/Entrega_Final.md`](final/Entrega_Final.md), [`final/Presentacion_Final.pptx`](final/Presentacion_Final.pptx), [`final/Final_Mineria.mp4`](final/Final_Mineria.mp4)
- Detalle de la carpeta: [`final/README.md`](final/README.md)

---

## Scripts locales

| Script | Para qué |
|---|---|
| [`scripts/generar_evidencias_final.py`](scripts/generar_evidencias_final.py) | Regenerar capturas en `final/evidencias/` |
| [`scripts/run_pipeline_gold_local.py`](scripts/run_pipeline_gold_local.py) | Correr pipeline gold en local (sin Colab) |

---

## Equipo

- Lucas Dugo
- Cristian Lo Giudice
- Rodolfo Berrone
- Santiago Ham Saguier
- Andrea Romeo
