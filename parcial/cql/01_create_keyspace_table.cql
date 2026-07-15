-- ============================================================
-- Segundo Parcial - Mineria de Datos II
-- Serving en Cassandra (AstraDB)
--
-- Equipo: Lucas Dugo, Cristian Lo Giudice, Rodolfo Berrone,
--         Santiago Ham Saguier, Andrea Romeo
--
-- NOTA: en AstraDB el keyspace se crea desde el panel web (no con CREATE KEYSPACE).
-- Usamos el keyspace por defecto "default_keyspace" para simplificar.
-- Esta tabla tambien se crea de forma programatica en el notebook 05 (astrapy),
-- pero dejamos el CQL para documentar el modelo y poder consultarlo en la CQL Console.
-- ============================================================

-- Tabla de serving para las consultas del parcial (gold examen_mineria_II).
-- Clave de particion: (org_id, usage_date) -> agrupa el uso de una org en un dia.
-- Clave de clustering: service -> identifica cada servicio dentro de esa particion.
-- Responde directo a las consultas #1 y #2 del enunciado.
CREATE TABLE IF NOT EXISTS default_keyspace.examen_mineria_II (
  org_id              text,
  usage_date          date,
  service             text,
  org_name            text,
  plan_tier           text,
  total_cost_usd      double,
  total_requests      bigint,
  total_genai_tokens  bigint,
  total_carbon_kg     double,
  event_count         int,
  PRIMARY KEY ((org_id, usage_date), service)
);
