# Evidencia del experimento

- `before.json`: layout original, antes de modificar Trusted.
- `after.json`: layout final (versión 3), con particiones, orden y bloom filters de 4 KiB.
- `resultados.md`: medianas y reducción de bytes entre esas dos fases.
- `parquet_verification.json`: snapshot y comprobación de offsets físicos de bloom filters en todos sus archivos.
- `bssid_cardinality.json`: consulta que justifica el tamaño del filtro para esta muestra.
- `after_sort_only.json`: medición intermedia de particiones/orden, sin bloom físico.
- `after_bloom_default.json`: medición intermedia que detectó la regresión con filtros de 1 MiB.

Los JSON guardan Query IDs reales de Athena. Abrir esas ejecuciones para capturar
sus estadísticas en la presentación; volver a ejecutar el SQL original sobre la
tabla ya optimizada produce una nueva medición y no reproduce el estado anterior.
Los tiempos fluctúan: comparar medianas y mostrar también los bytes leídos.

Las capturas del despliegue y de las migraciones RDS están en
[docs/evidencias_capturas/rds/](../../../evidencias_capturas/rds/).
Las capturas de [Glue](../../../evidencias_capturas/iceberg/) y
[Athena](../../../evidencias_capturas/athena/) complementan los resultados del experimento.
