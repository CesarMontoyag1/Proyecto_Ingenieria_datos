-- ============================================================================
-- Sprint 1 | Oficial de Gobernanza y Calidad
-- Guion de consultas Athena. Database: nexasecure_wids_catalog_dev_local
-- Ejecutar en orden. Capturar pantalla de cada resultado.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- BLOQUE 1 | PERFILAMIENTO: volumen y distribucion de clases
-- ---------------------------------------------------------------------------

-- 1.1 Volumen total ingerido en Raw
SELECT COUNT(*) AS total_registros_raw FROM raw_awid3;

-- 1.2 Distribucion trafico normal vs. malicioso (con porcentaje)
SELECT
    "Label",
    COUNT(*) AS registros,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
FROM raw_awid3
GROUP BY "Label"
ORDER BY registros DESC;

-- 1.3 Volumen que sobrevivio a los quality gates (Trusted)
SELECT COUNT(*) AS total_registros_trusted FROM trusted_awid3;


-- ---------------------------------------------------------------------------
-- BLOQUE 2 | COMPLETITUD sobre las 6 columnas criticas
-- Meta Sprint 0: > 98%. Las columnas salen de AWID3_CRITICAL_COLUMNS.
-- ---------------------------------------------------------------------------

SELECT
    'frame.time_epoch' AS columna,
    COUNT(*) AS total,
    COUNT("frame.time_epoch") AS no_nulos,
    ROUND(100.0 * COUNT("frame.time_epoch") / COUNT(*), 2) AS completitud_pct
FROM raw_awid3
UNION ALL SELECT 'frame.len', COUNT(*), COUNT("frame.len"),
    ROUND(100.0 * COUNT("frame.len") / COUNT(*), 2) FROM raw_awid3
UNION ALL SELECT 'wlan.bssid', COUNT(*), COUNT("wlan.bssid"),
    ROUND(100.0 * COUNT("wlan.bssid") / COUNT(*), 2) FROM raw_awid3
UNION ALL SELECT 'wlan.sa', COUNT(*), COUNT("wlan.sa"),
    ROUND(100.0 * COUNT("wlan.sa") / COUNT(*), 2) FROM raw_awid3
UNION ALL SELECT 'wlan.da', COUNT(*), COUNT("wlan.da"),
    ROUND(100.0 * COUNT("wlan.da") / COUNT(*), 2) FROM raw_awid3
UNION ALL SELECT 'Label', COUNT(*), COUNT("Label"),
    ROUND(100.0 * COUNT("Label") / COUNT(*), 2) FROM raw_awid3
ORDER BY completitud_pct ASC;


-- ---------------------------------------------------------------------------
-- BLOQUE 3 | QUARANTINE
-- OJO: el job escribe Parquet crudo SIN registrar tabla en el Glue Catalog.
-- Hay que crear la tabla externa antes de poder consultar.
-- Reemplazar <BUCKET_QUARANTINE> por el nombre real.
-- ---------------------------------------------------------------------------

-- 3.1 Registrar la tabla externa (una sola vez)
CREATE EXTERNAL TABLE IF NOT EXISTS quarantine_awid3 (
    rejection_reason string
)
STORED AS PARQUET
LOCATION 's3://nexasecure-wids-awid3-quarantine-dev-local/quarantine/batch/';

-- 3.2 Total de registros rechazados
SELECT COUNT(*) AS total_rechazados FROM quarantine_awid3;

-- 3.3 Distribucion por motivo de rechazo
-- Motivos posibles segun batch_ingest_job.py:
--   missing_critical_column:<col> | tcp_payload_overflow | tls_decryption_error
SELECT
    rejection_reason,
    COUNT(*) AS registros,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
FROM quarantine_awid3
GROUP BY rejection_reason
ORDER BY registros DESC;

-- 3.4 Agrupado por familia de motivo (colapsa el nombre de columna)
SELECT
    CASE WHEN rejection_reason LIKE 'missing_critical_column%'
         THEN 'missing_critical_column'
         ELSE rejection_reason END AS familia,
    COUNT(*) AS registros
FROM quarantine_awid3
GROUP BY 1
ORDER BY registros DESC;

-- 3.5 Tasa global de rechazo (cuadre Raw = Trusted + Quarantine)
SELECT
    (SELECT COUNT(*) FROM raw_awid3)        AS raw,
    (SELECT COUNT(*) FROM trusted_awid3)    AS trusted,
    (SELECT COUNT(*) FROM quarantine_awid3) AS quarantine,
    (SELECT COUNT(*) FROM raw_awid3)
      - (SELECT COUNT(*) FROM trusted_awid3)
      - (SELECT COUNT(*) FROM quarantine_awid3) AS descuadre;
-- descuadre debe ser 0. Si es negativo, Quarantine trae duplicados de
-- corridas anteriores (se escribe con mode="append", no createOrReplace).


-- ---------------------------------------------------------------------------
-- BLOQUE 4 | AUDITORIA DE SEUDONIMIZACION
-- Formato esperado en Trusted: OUI:OUI:OUI:<8 hex>  (ej. 00:11:22:a1b2c3d4)
-- Criterio de aprobacion: CERO MACs en texto plano.
-- ---------------------------------------------------------------------------

-- 4.1 Muestra visual para la captura del PDF
SELECT "wlan.sa", "wlan.da", "wlan.bssid" FROM trusted_awid3 LIMIT 10;

-- 4.2 CONTROL DURO: contar MACs completas en texto plano. Debe dar 0 en las tres.
SELECT
    SUM(CASE WHEN regexp_like("wlan.sa",    '^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$') THEN 1 ELSE 0 END) AS mac_plana_sa,
    SUM(CASE WHEN regexp_like("wlan.da",    '^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$') THEN 1 ELSE 0 END) AS mac_plana_da,
    SUM(CASE WHEN regexp_like("wlan.bssid", '^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$') THEN 1 ELSE 0 END) AS mac_plana_bssid
FROM trusted_awid3;

-- 4.3 Cobertura: cuantas cumplen el formato seudonimizado esperado
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN regexp_like("wlan.sa", '^([0-9a-fA-F]{2}:){3}[0-9a-fA-F]{8}$') THEN 1 ELSE 0 END) AS formato_ok,
    SUM(CASE WHEN regexp_like("wlan.sa", '^[0-9a-fA-F]{64}$') THEN 1 ELSE 0 END) AS hash_completo_64,
    SUM(CASE WHEN "wlan.sa" IS NULL THEN 1 ELSE 0 END) AS nulos
FROM trusted_awid3;
-- hash_completo_64 = MACs que no tenian 6 octetos: el codigo las hashea enteras
-- y pierde el OUI. Si es > 0, documentarlo como excepcion.

-- 4.4 Verificar que el OUI (fabricante) SI se conserva:
--     el conteo de prefijos distintos debe coincidir entre Raw y Trusted.
SELECT
    (SELECT COUNT(DISTINCT substr("wlan.sa", 1, 8)) FROM raw_awid3)     AS oui_distintos_raw,
    (SELECT COUNT(DISTINCT substr("wlan.sa", 1, 8)) FROM trusted_awid3) AS oui_distintos_trusted;

-- 4.5 Confirmar que NINGUN sufijo de Raw sobrevivio en Trusted
SELECT COUNT(*) AS sufijos_filtrados
FROM trusted_awid3 t
WHERE EXISTS (
    SELECT 1 FROM raw_awid3 r
    WHERE substr(r."wlan.sa", 10) = substr(t."wlan.sa", 10)
);
-- Debe dar 0 o un numero despreciable (colisiones fortuitas).


-- ---------------------------------------------------------------------------
-- BLOQUE 5 | VERIFICACION DEL BUG SOSPECHADO EN LA REGLA DE TLS
-- El job compara contra el string "1", pero lee con inferSchema=True.
-- Si Spark infiere esa columna como entero, la regla NUNCA se dispara.
-- ---------------------------------------------------------------------------

-- 5.1 Cuantos registros en Raw DEBERIAN haber sido rechazados por TLS
SELECT COUNT(*) AS candidatos_tls
FROM raw_awid3
WHERE CAST("tls.decryption_failed" AS varchar) = '1'
   OR "tls.alert_message.desc" = 'bad_record_mac';

-- 5.2 Comparar contra lo que realmente cayo en Quarantine por ese motivo
SELECT COUNT(*) AS rechazados_tls
FROM quarantine_awid3
WHERE rejection_reason = 'tls_decryption_error';
-- Si 5.1 > 0 y 5.2 = 0, el bug esta confirmado: es un fallo del control de
-- calidad, no que los datos esten limpios.
