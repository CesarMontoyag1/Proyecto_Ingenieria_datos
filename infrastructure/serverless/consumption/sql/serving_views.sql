-- Capa Serving de NexaSecure WIDS: vistas de Athena que consume QuickSight.
--
-- Reglas de gobernanza:
--   * Solo leen trusted_awid3 (seudonimizada) y alerts_serving. Nunca Raw ni Quarantine.
--   * Ninguna columna contiene una MAC en texto plano.
--   * vw_top_bssid suprime los BSSID con menos de 5 tramas (k >= 5, Sprint 0).
--
-- Las crea scripts/athena_views.py, que reemplaza {database} por la base del catálogo.
-- Cada sentencia termina en ";" al final de una línea.

CREATE OR REPLACE VIEW "{database}"."vw_alerts" AS
SELECT
    alert_id,
    rule_type,
    severity,
    CASE severity
        WHEN 'CRITICAL' THEN 4
        WHEN 'HIGH' THEN 3
        WHEN 'MEDIUM' THEN 2
        ELSE 1
    END AS severity_rank,
    CASE WHEN severity IN ('HIGH', 'CRITICAL') THEN 1 ELSE 0 END AS is_high_or_critical,
    bssid_pseudo,
    src_mac_pseudo,
    observed_count,
    threshold,
    window_seconds,
    CAST(from_iso8601_timestamp(detected_at) AS timestamp) AS detected_at,
    detection_latency_ms,
    CASE WHEN detection_latency_ms < 2000 THEN 'Cumple' ELSE 'No cumple' END AS sla_status,
    -- Promediada en el dashboard da el % de alertas que cumplen el SLA de 2 s
    CASE WHEN detection_latency_ms < 2000 THEN 100 ELSE 0 END AS sla_ok_pct,
    CAST(dt AS date) AS detected_date
FROM "{database}"."alerts_serving";

CREATE OR REPLACE VIEW "{database}"."vw_alerts_hourly" AS
SELECT
    date_trunc('hour', detected_at) AS detected_hour,
    rule_type,
    severity,
    COUNT(*) AS alerts,
    COUNT(DISTINCT bssid_pseudo) AS affected_bssids,
    approx_percentile(detection_latency_ms, 0.5) AS latency_p50_ms,
    approx_percentile(detection_latency_ms, 0.95) AS latency_p95_ms,
    MAX(observed_count) AS max_observed_count
FROM "{database}"."vw_alerts"
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW "{database}"."vw_traffic_daily" AS
SELECT
    event_date,
    "Label" AS label,
    CASE WHEN "Label" = 'Normal' THEN 'Normal' ELSE 'Ataque' END AS traffic_class,
    CASE CAST(TRY_CAST("wlan.fc.type" AS double) AS integer)
        WHEN 0 THEN 'Gestión'
        WHEN 1 THEN 'Control'
        WHEN 2 THEN 'Datos'
        ELSE 'Desconocido'
    END AS frame_type,
    COUNT(*) AS frames,
    SUM(CASE WHEN "Label" <> 'Normal' THEN 1 ELSE 0 END) AS attack_frames,
    SUM(TRY_CAST("frame.len" AS double)) AS frame_bytes,
    approx_distinct("wlan.bssid") AS distinct_bssids
FROM "{database}"."trusted_awid3"
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW "{database}"."vw_top_bssid" AS
SELECT
    "wlan.bssid" AS bssid_pseudo,
    COUNT(*) AS frames,
    SUM(CASE WHEN "Label" <> 'Normal' THEN 1 ELSE 0 END) AS attack_frames,
    ROUND(100.0 * SUM(CASE WHEN "Label" <> 'Normal' THEN 1 ELSE 0 END) / COUNT(*), 2) AS attack_pct,
    COUNT(DISTINCT "Label") AS distinct_labels,
    MIN(event_date) AS first_seen,
    MAX(event_date) AS last_seen
FROM "{database}"."trusted_awid3"
WHERE "wlan.bssid" IS NOT NULL
  AND trim("wlan.bssid") <> ''
  AND lower("wlan.bssid") NOT LIKE 'ff:ff:ff%'
GROUP BY 1
HAVING COUNT(*) >= 5;
