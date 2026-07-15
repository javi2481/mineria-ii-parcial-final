-- ============================================================
-- 5 consultas minimas del enunciado final (CQL Console AstraDB)
-- Guardar capturas en evidencias/
-- Keyspace: default_keyspace (igual parcial2)
-- ============================================================

-- #1 Costos y requests diarios por org y servicio en un rango de fechas
SELECT usage_date, service, total_cost_usd, total_requests, event_count
FROM default_keyspace.org_daily_usage_by_service
WHERE org_id = 'org_pbhsahxt'
  AND usage_date >= '2025-07-01'
  AND usage_date <= '2025-08-31';

-- #2 Top servicios por costo acumulado (~14 dias) para una org
-- (en CQL puro: consultar rango y agregar en cliente; el notebook hace el top-N)
SELECT service, sum(total_cost_usd) AS costo_acumulado
FROM default_keyspace.org_daily_usage_by_service
WHERE org_id = 'org_pbhsahxt'
  AND usage_date >= '2025-08-05'
  AND usage_date <= '2025-08-18'
GROUP BY service;

-- #3 Tickets criticos/altos y SLA breach (ultimos 30 dias)
SELECT ticket_date, severity, ticket_count, critical_count, sla_breach_rate
FROM default_keyspace.tickets_by_org_date
WHERE org_id = 'org_pbhsahxt'
  AND severity IN ('critical', 'high')
  AND ticket_date >= '2025-07-19'
  AND ticket_date <= '2025-08-18';

-- #4 Revenue mensual con creditos/impuestos en USD
SELECT month, revenue_usd, total_credits, total_taxes, currency
FROM default_keyspace.revenue_by_org_month
WHERE org_id = 'org_pbhsahxt';

-- #5 Tokens GenAI y costo estimado por dia
SELECT usage_date, total_tokens, estimated_cost_usd
FROM default_keyspace.genai_tokens_by_org_date
WHERE org_id = 'org_pbhsahxt'
  AND usage_date >= '2025-07-01';
