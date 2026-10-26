# Optimización Iceberg — entregable de Daniel

## Alcance

Trusted incorpora `event_date DATE`, calculada a partir de `frame.time_epoch`
en UTC, particiones de identidad por `event_date` y `Label`, orden de escritura
por `wlan.bssid`, estadísticas completas de esa columna y bloom filter Parquet.
Raw no cambia. El streaming continúa escribiendo NDJSON fuera de esta tabla;
integrarlo en Iceberg corresponde al pipeline de Luis.

Glue 4.0 incluye Iceberg 1.0.0. Su implementación Parquet soporta
`write.parquet.bloom-filter-enabled.column.wlan.bssid`; esto configura el escritor,
sin garantizar que Athena use el filtro. El benchmark mide el efecto conjunto
de particiones, distribución, orden y archivos reescritos.

En los archivos de esta versión, Iceberg codifica `wlan.bssid` como
`wlan_x2Ebssid`. Se configura también el bloom filter sobre ese nombre físico;
declararlo únicamente con el nombre lógico no lo escribe. El layout final se
identifica como versión `3`. El tamaño máximo es 4 KiB por filtro: el valor
predeterminado de 1 MiB aumentó la lectura de la consulta BSSID de 238.436 a
6.394.130 bytes, por lo que se conserva esa medición intermedia y se ajusta el
tamaño a la cardinalidad de BSSIDs de esta muestra. `scripts/verify_iceberg.py` recorre los manifests del
snapshot y comprueba offsets reales de bloom filters en los footers Parquet,
sin descargar filas de eventos ni depender de las tablas de metadatos de Athena.
La consulta de cardinalidad aproximada devolvió 120 valores distintos; su SQL y
Query ID quedan en `evidencia/bssid_cardinality.json`. Reevaluar el tamaño si la
cardinalidad crece sustancialmente con nuevas cargas.

## Secuencia

Detener ingesta batch y mantenimiento durante todo el experimento, y coordinar
con Luis que no arranque otra ejecución. El guard del script comprueba jobs
activos y clústeres EMR de NexaSecure antes de iniciar la migración. No representa
un bloqueo distribuido sobre escritores externos: mantener esa exclusión operativa.

Desde la raíz:

```powershell
.venv/Scripts/python.exe scripts/iceberg_benchmark.py before
.venv/Scripts/python.exe scripts/glue.py package
.venv/Scripts/python.exe scripts/glue.py deploy
.venv/Scripts/python.exe scripts/glue.py optimize
.venv/Scripts/python.exe scripts/glue.py status
# Esperar SUCCEEDED antes de continuar:
.venv/Scripts/python.exe scripts/iceberg_benchmark.py after
.venv/Scripts/python.exe scripts/verify_iceberg.py
```

El benchmark descubre fecha/etiqueta y un BSSID no vacío que exista en los datos.
Ejecuta tres mediciones por consulta y fase, con reutilización desactivada.
Las consultas temporales de ambas fases son equivalentes: antes calculan la fecha
desde el epoch; después filtran `event_date`, permitiendo pruning de particiones.
La consulta de BSSID es idéntica en ambas fases. Compara filas y suma de bytes de
trama, y aborta si los resultados difieren o cambia el snapshot durante una fase.

`evidencia/before.json` y `after.json` contienen SQL, tiempos, bytes, resultados,
identificadores de consulta y metadatos Iceberg. `resultados.md` resume medianas.
No se sobrescribe evidencia: otro experimento necesita un nuevo `--directory`.
El guard de `glue.py optimize` usa la carpeta predeterminada del experimento.
La ubicación de resultados es `s3://<trusted>/athena-results/optimization/`;
no necesita cambiar el workgroup `primary`.

## Migración de la tabla existente

`TrustedOptimizationJob` hace lo siguiente:

1. Lee un snapshot fijo de Trusted y crea `trusted_awid3_baseline`, sin particiones
   ni orden, en un prefijo separado. Incluye la fecha derivada y registra el ID
   del snapshot original en las propiedades.
2. Compara los multiconjuntos de las 254 columnas originales (incluye duplicados)
   y comprueba que no hubo nuevas escrituras. Rechaza epochs que no produzcan fecha.
3. Agrega la columna y las particiones a la tabla existente, configura orden y bloom
   filter, y sobrescribe todos sus registros desde la baseline en un nuevo snapshot.
4. Verifica igualdad completa de filas entre Trusted y la baseline antes de reportar
   éxito. No modifica Raw ni sus bookmarks y no reingresa los CSV.

Si falla tras configurar el esquema, no reactivar batch: corregir y repetir el job.
La baseline permite reintentar con los mismos datos; si Trusted cambió, aborta.
No ejecutar compactación ni expiración de snapshots hasta terminar la validación.
Para recuperación forense, conservar baseline, ID del snapshot original y logs.
No eliminar carpetas de datos o metadatos manualmente.

La ingesta nueva usa `iceberg_layout.py`, entregado con `--extra-py-files`, y calcula
`event_date` en cada lote. Si Trusted existía sin migrar, falla antes de escribir Raw
para evitar una actualización parcial por este cambio de layout.

Compatibilidad verificada en Glue 4.0 / Spark 3.3: la detección de tablas usa
`SHOW TABLES` porque `Catalog.tableExists` no resuelve correctamente los nombres
de tres partes en este entorno. La comparación usa `exceptAll(...).take(1)`;
`count()` sobre `exceptAll` falla por poda de columnas en esta versión de Spark.
La reescritura usa SQL `INSERT OVERWRITE` en modo estático: el wrapper Python
`DataFrameWriterV2.overwrite(Column)` de Glue 4.0 no convierte la condición a Java.

## Consultas para la presentación

Usar el SQL exacto guardado en los JSON de evidencia y buscar sus Query IDs en
Athena. Capturar estadísticas y resultados de ambas fases. Comparar bytes leídos
y mediana del tiempo del motor; no afirmar mejoras basándose en una sola corrida.
Los datos AWID3 son históricos: no filtrar por la fecha actual.

## Referencias

- [Iceberg 1.0.0: propiedades Parquet](https://github.com/apache/iceberg/blob/apache-iceberg-1.0.0/core/src/main/java/org/apache/iceberg/TableProperties.java)
- [Iceberg: evolución de particiones y orden de escritura](https://iceberg.apache.org/docs/latest/spark-ddl/)
- [Athena: reutilización de resultados](https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html)
