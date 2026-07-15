# Segundo Parcial — Cloud Provider Analytics

Notebook y CQL del segundo parcial. Pipeline batch + streaming hasta una tabla gold en Astra.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/javi2481/mineria-ii-parcial-final/blob/main/parcial/parcial2_colab.ipynb)

## Contenido

| Archivo / carpeta | Qué es |
|---|---|
| [`parcial2_colab.ipynb`](parcial2_colab.ipynb) | Notebook principal |
| [`cql/`](cql/) | DDL y consultas de prueba |
| [`docs/decision_log.md`](docs/decision_log.md) | Decisiones técnicas del parcial |

## Cómo correr

1. Abrí el notebook en Colab (badge de arriba).
2. Cargá el secret **`ASTRA_TOKEN`** (cluster `db_rodolfo_istea`).
3. **Entorno de ejecución → Ejecutar todo**.

## Astra

- Tabla: `default_keyspace.examen_mineria_II`
- Consultas: [`cql/02_queries.cql`](cql/02_queries.cql)
