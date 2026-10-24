-- NexaSecure WIDS — Optimización de la capa transaccional (índices)
-- Implementa EXACTAMENTE lo prometido en la sección 3.2 (Componente 2) del
-- Sprint 0: "índice primario disperso sobre la clave de ordenamiento,
-- índices de salto minmax y bloom filter... e índices BRIN, parciales y
-- GIN sobre la capa transaccional de alertas."
--
-- Esta es la optimización #2 de las 2 exigidas en la rúbrica del Sprint 2
-- (la #1 es el particionamiento + bloom filter de Daniel sobre Iceberg).
-- Capturar el plan ANTES de correr este archivo (ver demo_indexes.py) para
-- poder mostrar el comparativo en la presentación.

BEGIN;

-- BRIN sobre detected_at: alerts es append-only y se escribe en orden
-- cronológico (el job de Flink inserta en tiempo real), que es exactamente
-- el caso de uso ideal para BRIN -- ocupa una fracción del espacio de un
-- B-tree y acelera los filtros por rango de fecha que usa el dashboard SOC
-- ("alertas de las últimas 24h").
CREATE INDEX idx_alerts_detected_at_brin
    ON alerts USING BRIN (detected_at)
    WITH (pages_per_range = 32);

-- GIN sobre details (JSONB): details guarda metadatos variables por tipo de
-- regla (ej. canal Wi-Fi, SSID observado, lista de BSSIDs vecinos). Permite
-- búsquedas por clave/valor dentro del JSON sin escanear toda la tabla,
-- necesario para investigación forense de un IoC específico.
CREATE INDEX idx_alerts_details_gin
    ON alerts USING GIN (details);

-- Índice parcial sobre incident_cases: el analista SOC casi siempre
-- consulta "¿qué está abierto ahora mismo?", no el histórico completo. Un
-- índice parcial solo indexa esas filas, que son una fracción minúscula del
-- total una vez el sistema lleve meses operando -- más rápido y más
-- liviano que indexar status completo.
CREATE INDEX idx_incident_cases_open
    ON incident_cases (opened_at)
    WHERE status IN ('open', 'investigating');

-- Índice compuesto sobre alerts (rule_type, severity): soporta la consulta
-- más común del dashboard -- "alertas HIGH/CRITICAL de tipo X" -- sin
-- necesitar un índice separado por columna.
CREATE INDEX idx_alerts_rule_severity
    ON alerts (rule_type, severity);

COMMIT;
