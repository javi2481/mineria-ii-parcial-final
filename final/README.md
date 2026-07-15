# Examen Final

## Equipo

- Lucas Dugo
- Cristian Lo Giudice
- Rodolfo Berrone
- Santiago Ham Saguier
- Andrea Romeo

Carpeta con la entrega del examen final: notebook, documentación, CQL, evidencias y archivos de presentación.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/javi2481/mineria-ii-parcial-final/blob/main/final/final_colab.ipynb)

## Contenido

| Archivo / carpeta | Qué es |
|---|---|
| [`final_colab.ipynb`](final_colab.ipynb) | Notebook principal (PySpark en Colab) |
| [`Entrega_Final.md`](Entrega_Final.md) | Documento de entrega |
| [`Presentacion_Final.pptx`](Presentacion_Final.pptx) | Presentación del equipo |
| [`Final_Mineria.mp4`](Final_Mineria.mp4) | Video de la entrega |
| [`cql/`](cql/) | DDL de tablas y 5 consultas del enunciado |
| [`docs/decision_log.md`](docs/decision_log.md) | Log de decisiones técnicas |
| [`evidencias/`](evidencias/) | Capturas de las 5 consultas CQL |

## Cómo correr

1. Abrí el notebook en Colab (badge de arriba).
2. Cargá el secret **`ASTRA_TOKEN_FINAL`** (cluster `db_final_istea`).
3. **Entorno de ejecución → Ejecutar todo**.

El notebook clona el repo, procesa `datalake/landing/` y deja salidas en `final/datalake/` (no van al repo; están en `.gitignore`).

## Astra

- Keyspace: `default_keyspace`
- 5 tablas gold cargadas con `foreachBatch`
- CQL de referencia: [`cql/01_create_keyspace_table.cql`](cql/01_create_keyspace_table.cql) y [`cql/02_queries.cql`](cql/02_queries.cql)
