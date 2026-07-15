-- ============================================================
-- Entrega Final - Mineria de Datos II
-- Serving en Cassandra (AstraDB)
--
-- NOTA: en AstraDB el keyspace se crea desde el panel web (no con CREATE KEYSPACE).
-- Usamos el keyspace por defecto "default_keyspace" para simplificar (igual parcial2).
-- Las tablas tambien se crean de forma programatica en final_colab.ipynb (astrapy).
-- ============================================================

-- Consulta #1 y #2: costos/requests diarios y top servicios por org
CREATE TABLE IF NOT EXISTS default_keyspace.org_daily_usage_by_service (
    org_id text,
    usage_date date,
    service text,
    org_name text,
    plan_tier text,
    total_cost_usd double,
    total_requests bigint,
    total_cpu_hours double,
    total_storage_gb_hours double,
    total_genai_tokens bigint,
    total_carbon_kg double,
    event_count int,
    PRIMARY KEY ((org_id, usage_date), service)
);

-- Consulta #4: revenue mensual USD
CREATE TABLE IF NOT EXISTS default_keyspace.revenue_by_org_month (
    org_id text,
    month date,
    org_name text,
    plan_tier text,
    revenue_usd double,
    total_credits double,
    total_taxes double,
    currency text,
    PRIMARY KEY (org_id, month)
);

-- FinOps: anomalias de costo
CREATE TABLE IF NOT EXISTS default_keyspace.cost_anomaly_mart (
    org_id text,
    usage_date date,
    service text,
    org_name text,
    spike_event_count int,
    max_cost_usd double,
    avg_cost_usd double,
    anomaly_method text,
    is_anomaly boolean,
    PRIMARY KEY ((org_id, usage_date), service)
);

-- Consulta #3: tickets por org/fecha/severidad
CREATE TABLE IF NOT EXISTS default_keyspace.tickets_by_org_date (
    org_id text,
    ticket_date date,
    severity text,
    ticket_count int,
    critical_count int,
    sla_breach_rate double,
    avg_csat double,
    PRIMARY KEY (org_id, ticket_date, severity)
);

-- Consulta #5: tokens GenAI por dia
CREATE TABLE IF NOT EXISTS default_keyspace.genai_tokens_by_org_date (
    org_id text,
    usage_date date,
    org_name text,
    total_tokens bigint,
    estimated_cost_usd double,
    PRIMARY KEY (org_id, usage_date)
);
