-- ============================================================
-- Consultas minimas del enunciado (ejecutar en la CQL Console de AstraDB)
-- Guardar capturas de los resultados en la carpeta evidencias/
--
-- Equipo: Lucas Dugo, Cristian Lo Giudice, Rodolfo Berrone,
--         Santiago Ham Saguier, Andrea Romeo
-- ============================================================

-- Consulta #1: todos los servicios usados por una organizacion en un dia.
-- Usa la clave de particion completa (org_id + usage_date) -> consulta eficiente.
SELECT service, total_cost_usd, total_requests, total_genai_tokens, event_count
FROM default_keyspace.examen_mineria_II
WHERE org_id = 'org_pbhsahxt' AND usage_date = '2025-08-01';

-- Consulta #2: evolucion del costo de un servicio (compute) en un rango de fechas
-- para una organizacion. El rango aplica sobre usage_date (parte de la PK).
SELECT usage_date, service, total_cost_usd, event_count
FROM default_keyspace.examen_mineria_II
WHERE org_id = 'org_pbhsahxt'
  AND usage_date >= '2025-07-01'
  AND usage_date <= '2025-08-31'
  AND service = 'compute'
ALLOW FILTERING;
