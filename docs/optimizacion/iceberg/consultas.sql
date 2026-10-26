-- Referencia de consultas; el benchmark genera los filtros con datos reales.
-- Database: nexasecure_wids_catalog_dev_local. Reutilización desactivada.

-- ANTES: fecha derivada sobre el layout original.
SELECT COUNT(*) AS records, SUM(TRY_CAST("frame.len" AS double)) AS frame_bytes
FROM trusted_awid3
WHERE CAST(from_unixtime(TRY_CAST("frame.time_epoch" AS double)) AS date) = DATE '2020-12-18'
  AND "Label" = 'Normal';

-- DESPUÉS: mismo filtro, con columna de partición.
SELECT COUNT(*) AS records, SUM(TRY_CAST("frame.len" AS double)) AS frame_bytes
FROM trusted_awid3
WHERE event_date = DATE '2020-12-18' AND "Label" = 'Normal';

-- ANTES/DESPUÉS: usar el BSSID seudonimizado que figure en evidencia/before.json.
-- SELECT COUNT(*), SUM(TRY_CAST("frame.len" AS double))
-- FROM trusted_awid3 WHERE "wlan.bssid" = '<BSSID_DE_EVIDENCIA>';
