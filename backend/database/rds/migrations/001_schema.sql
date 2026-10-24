-- NexaSecure WIDS — Capa transaccional (RDS PostgreSQL)
-- Cumple el Componente 2 (BD Avanzadas) prometido en el Sprint 0:
-- motor transaccional para alertas, casos de incidente, catálogo de reglas
-- y bitácora de auditoría. CP bajo CAP: una duplicación aquí causaría doble
-- respuesta a incidente, así que se prioriza consistencia sobre disponibilidad.
--
-- Aplicar con yoyo-migrations o psql directo:
--   psql -h <host> -U <user> -d nexasecure -f 001_schema.sql

BEGIN;

-- Catálogo de reglas de detección (Deauth Flood, Rogue AP, Disas Burst, ...)
-- Alimenta flink-app/src/detection_rules.py en lugar de tener los umbrales
-- harcodeados: permite recalibrar sensibilidad sin redeploy del job de Flink.
CREATE TABLE rule_catalog (
    id              SERIAL PRIMARY KEY,
    rule_type       TEXT NOT NULL UNIQUE,
    description     TEXT NOT NULL,
    default_severity TEXT NOT NULL CHECK (default_severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    default_threshold INTEGER NOT NULL,
    default_window_seconds INTEGER NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Alertas generadas por flink-app/src/detection_job.py -> alert_formatter.
-- Append-only por diseño: una alerta nunca se edita, solo se referencia
-- desde incident_cases. bssid/src_mac llegan ya pseudonimizados (HMAC),
-- nunca en texto plano -- coherente con la Ley 1581 de 2012.
CREATE TABLE alerts (
    id                  BIGSERIAL PRIMARY KEY,
    rule_type           TEXT NOT NULL REFERENCES rule_catalog(rule_type),
    severity            TEXT NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    bssid_pseudo         TEXT NOT NULL,
    src_mac_pseudo       TEXT,
    observed_count       INTEGER,
    threshold            INTEGER,
    window_seconds       INTEGER,
    detected_at          TIMESTAMPTZ NOT NULL,
    details              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Casos de incidente abiertos por el analista SOC a partir de una o más alertas.
CREATE TABLE incident_cases (
    id              BIGSERIAL PRIMARY KEY,
    alert_id        BIGINT NOT NULL REFERENCES alerts(id),
    assigned_to     TEXT,
    status          TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'investigating', 'closed', 'false_positive')),
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at       TIMESTAMPTZ,
    resolution_notes TEXT
);

-- Bitácora de auditoría inmutable: quién consultó qué. Exigida en la
-- sección 3.2 (Componente 3) del Sprint 0 ("bitácora de auditoría inmutable
-- de cada consulta"). Nunca se actualiza ni se borra (ver readonly_role.sql
-- para el rol que solo puede hacer INSERT/SELECT sobre esta tabla).
CREATE TABLE audit_log (
    id              BIGSERIAL PRIMARY KEY,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    target_id       BIGINT,
    performed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    details         JSONB NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO rule_catalog (rule_type, description, default_severity, default_threshold, default_window_seconds) VALUES
    ('DEAUTH_FLOOD', 'Ráfaga de tramas de deautenticación hacia un mismo BSSID en una ventana corta', 'HIGH', 10, 5),
    ('ROGUE_AP', 'Beacon con SSID conocido emitido desde un BSSID no autorizado', 'CRITICAL', 1, 1),
    ('DISAS_BURST', 'Ráfaga de tramas de desasociación, posible interrupción de servicio', 'MEDIUM', 10, 5);

COMMIT;
