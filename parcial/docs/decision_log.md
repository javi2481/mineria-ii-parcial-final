# Apuntes de decisiones — Segundo Parcial Minería de Datos II

**Equipo:** Lucas Dugo, Cristian Lo Giudice, Rodolfo Berrone, Santiago Ham Saguier, Andrea Romeo.

- **Batch** para los 3 CSV maestros; **streaming** para los JSONL de eventos (lo pide el parcial).
- Esquemas **a mano** (`StructType`), sin `inferSchema`.
- En bronze, `value` va como **String** porque a veces viene número y a veces texto; en silver lo paso a double.
- Silver y gold particionados por **`usage_date`**.
- Tabla gold / Astra: **`examen_mineria_II`** con PK **`(org_id, usage_date)`** + clustering **`service`** — para las consultas del enunciado.
- Reglas silver: **R1** event_id nulo, **R2** costo < -0.01, **R3** value sin unit. Lo malo → quarantine.
- Conexión a Astra con **astrapy** (token + endpoint, sin bundle).
- Si el stream falla al re-ejecutar en Colab, limpio carpeta de salida y checkpoint (`rm -rf`).
- **Astra UNKNOWN_TABLE_COLUMNS**: `drop_table` + `create_table` (no `if_not_exists`) — tabla vieja tenía 5 columnas.
- **Astra TableInsertManyException**: `insert_many(..., ordered=True, concurrency=1)`; borrar collection homónima si existe.
- **Astra DataAPITimeoutException**: insertar en lotes de 10 filas con reintento (timeout 30s de astrapy).
- Notebook en tono estudiante: bloque markdown breve (≤3 líneas) antes de cada celda de código; lógica lineal por capas.
