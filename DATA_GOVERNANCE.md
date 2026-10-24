# Data Governance — NexaSecure WIDS

Este documento mapea cada dato, tabla y responsabilidad del pipeline descrito en `README.md`, siguiendo la estructura exigida en la rúbrica del Sprint 2 (diccionario de datos, catálogo de datos, linaje, matriz RACI, reglas de calidad ya implementadas).

- [Diccionario de datos](#diccionario-de-datos)
- [Catálogo de datos](#catálogo-de-datos)
- [Linaje](#linaje)
- [Matriz RACI](#matriz-raci)
- [Reglas de calidad ya implementadas](#reglas-de-calidad-ya-implementadas)

## Diccionario de datos

### Dataset fuente: AWID3-CSV (254 columnas, 6 familias)

El esquema completo vive en `backend/src/common/awid3_schema.py` (`AWID3_COLUMNS`). Se agrupa en 6 familias funcionales:

| Familia | Prefijo | Ejemplos | Sensibilidad |
| --- | --- | --- | --- |
| Radiotap | `radiotap.*` | `radiotap.channel.freq`, `radiotap.dbm_antsignal` | Ninguna |
| Frame | `frame.*` | `frame.time_epoch`, `frame.len`, `frame.number` | Ninguna |
| WLAN | `wlan.*` | `wlan.bssid`, `wlan.sa`, `wlan.da`, `wlan.ssid` | **Alta** — `wlan.bssid`, `wlan.sa`, `wlan.da`, `wlan.ra`, `wlan.ta` son direcciones MAC, dato identificable bajo la Ley 1581 de 2012 |
| WLAN Radio | `wlan_radio.*` | `wlan_radio.signal_dbm`, `wlan_radio.channel` | Ninguna |
| Red y transporte | `tcp.*`, `ip.*` | `tcp.payload`, `ip.addr` | Media — `tcp.payload` puede contener fragmentos de contenido de aplicación |
| Aplicación / etiqueta | — | `Label` | Ninguna (es la etiqueta normal/ataque del dataset) |

### Zona Raw (S3, tabla Iceberg `raw_awid3`)

Copia inmutable del CSV fuente, sin ningún filtro ni transformación. Las 254 columnas en texto plano, incluidas las MAC. Zona restringida: no consultable por personas, solo por el job batch/streaming que la escribe.

### Zona Trusted (S3, tabla Iceberg `trusted_awid3`)

Igual esquema que Raw, excepto que `wlan.bssid`, `wlan.sa`, `wlan.da`, `wlan.ra`, `wlan.ta` están seudonimizadas con HMAC-SHA256 (ver [Reglas de calidad](#reglas-de-calidad-ya-implementadas)). Confidencial: identificadores ya seudonimizados, consultable por el equipo de desarrollo y por Athena/Redshift para análisis.

### Zona Quarantine (S3, Parquet `quarantine_awid3`)

Registros que no pasaron la validación, con la columna adicional `rejection_reason` para auditar por qué. Mismo nivel de sensibilidad que Raw (no seudonimizado) — acceso igualmente restringido.

### RDS PostgreSQL (`nexasecure`, esquema `public`)

| Tabla | Columnas clave | Notas |
| --- | --- | --- |
| `rule_catalog` | `rule_type`, `default_severity`, `default_threshold`, `default_window_seconds` | Catálogo de las reglas de detección (Deauth Flood, Rogue AP, Disas Burst) |
| `alerts` | `rule_type` (FK), `severity`, `bssid_pseudo`, `src_mac_pseudo`, `detected_at`, `details` (JSONB) | Append-only. `bssid_pseudo`/`src_mac_pseudo` llegan ya seudonimizados desde `flink-app` — RDS nunca recibe una MAC en texto plano |
| `incident_cases` | `alert_id` (FK), `status`, `assigned_to`, `resolution_notes` | Casos abiertos por el analista SOC a partir de una alerta |
| `audit_log` | `actor`, `action`, `target_table`, `performed_at` | Bitácora inmutable de consultas sensibles (ver [RBAC](#reglas-de-calidad-ya-implementadas)) |

Ninguna tabla de RDS contiene una dirección MAC en texto plano ni ningún otro campo PII del dataset original.

## Catálogo de datos

| Dataset | Vive en | Alimentado por | Contiene PII |
| --- | --- | --- | --- |
| `raw_awid3` | S3 (Iceberg), zona Raw | `glue-jobs/src/batch_ingest_job.py`, Kinesis Firehose (streaming) | Sí, en texto plano |
| `trusted_awid3` | S3 (Iceberg), zona Trusted | `glue-jobs/src/batch_ingest_job.py`, `flink-app/src/detection_job.py` | No, seudonimizado |
| `quarantine_awid3` | S3 (Parquet), zona Quarantine | Mismo origen que Raw, registros rechazados | Sí, en texto plano |
| `rule_catalog`, `alerts`, `incident_cases`, `audit_log` | RDS PostgreSQL `nexasecure` | `flink-app/src/detection_job.py` (alerts), analistas SOC (incident_cases) | No |

Credenciales también forman parte del catálogo porque definen quién puede leer qué: `HmacSecret` (Secrets Manager, usada para seudonimizar), `MasterCredentials` y los roles `security_analyst`/`auditor` de RDS (ver `database/rds/migrations/003_readonly_role.sql`).

## Linaje

1. El dataset AWID3-CSV llega por dos rutas: batch (`kaggle_batch_loader.py` sube el CSV a `landing/batch/`) y streaming (`kaggle_stream_simulator.py` inyecta eventos a Kinesis Data Stream).
2. Ruta batch: `glue-jobs/src/batch_ingest_job.py` escribe una copia sin filtrar en `raw_awid3`, valida cada fila con `_validate_record_logic` (calibrada por tipo de trama 802.11), seudonimiza las MAC válidas con HMAC-SHA256 y escribe en `trusted_awid3`; las filas rechazadas van a `quarantine_awid3` con su `rejection_reason`.
3. Ruta streaming: Kinesis Firehose persiste el evento crudo en `raw/streaming/` sin transformar. `flink-app/src/detection_job.py` consume el stream, valida, seudonimiza (mismo algoritmo HMAC-SHA256, misma llave en Secrets Manager que el job batch) y escribe en `trusted_awid3`, evaluando en paralelo las reglas de `detection_rules.py`.
4. Si una regla dispara, `detection_job.py` inserta una fila en `alerts` (RDS) y publica a la cola SQS de alertas; `backend/src/alert_formatter/handler.py` la formatea y notifica (SNS/Slack).
5. El analista SOC abre un `incident_cases` a partir de una alerta, y cada consulta que hace sobre `alerts`/`incident_cases` con el rol `security_analyst` queda escrita en `audit_log`.
6. Las consultas analíticas (Athena/Redshift, Sprint Final) leen exclusivamente de `trusted_awid3`, nunca de Raw ni Quarantine.

El punto exacto donde se aplica la seudonimización (paso 2 y 3) es la evidencia auditable de que ningún dato aguas abajo de Trusted expone una MAC real.

## Matriz RACI

Roles tomados de la matriz de stakeholders del Sprint 0 (sección 2.3):

- **CISO / Gerente de Seguridad**: dueño del negocio, visibilidad y priorización de inversión en seguridad.
- **Arquitecto de Datos/Cloud**: diseña y opera la infraestructura y los pipelines.
- **Oficial de Cumplimiento/DPO**: garantiza el cumplimiento de la Ley 1581 de 2012 y aprueba accesos.
- **Analista SOC**: consume las alertas y abre/cierra casos de incidente.

| Actividad | CISO | Arquitecto Cloud | Oficial Cumplimiento/DPO | Analista SOC |
| --- | --- | --- | --- | --- |
| Diseñar y mantener las reglas de validación de calidad | I | R, A | C | I |
| Seudonimizar y proteger las MAC (PII) | I | R | A | |
| Decidir qué campos se excluyen o enmascaran por PII | | R | A | |
| Diseñar y operar el pipeline (Glue, Flink, RDS) | I | R, A | | I |
| Aprobar el acceso de solo lectura de los roles `security_analyst`/`auditor` | | R | A | I |
| Consultar alertas y gestionar casos de incidente | | | | R, A |
| Responder a un incidente de seguridad o filtración de datos | C | R | A | R |
| Revisar y aprobar cambios al esquema de datos | C | R | A | I |
| Auditar la bitácora de consultas sensibles | | I | R, A | I |

R: Responsable. A: Aprueba/dueño. C: Consultado. I: Informado.

## Reglas de calidad ya implementadas

Las reglas viven en código, no en este documento — esta sección es un resumen legible. Fuente de verdad: `glue-jobs/src/schema_validation.py`, `glue-jobs/src/pseudonymize.py`, `flink-app/src/schema_validation.py`, `flink-app/src/pseudonymize.py`.

Validación de registros (idéntica en batch y streaming, calibrada por tipo de trama 802.11 — `wlan.fc.type`):

- Columnas universales obligatorias en toda trama: `frame.time_epoch`, `frame.len`, `Label`.
- Tramas de control (`wlan.fc.type = 1`, ej. ACK/RTS/CTS): no exigen `wlan.bssid` por diseño del estándar 802.11 — exigen `wlan.ra` o `wlan.da`.
- Tramas de gestión y datos (`wlan.fc.type` 0 o 2): exigen `wlan.bssid` y al menos `wlan.sa` o `wlan.ta`.
- `tcp.payload` no puede exceder 32.767 caracteres (desbordamiento).
- Se rechaza cualquier trama con `tls.alert_message.desc = "bad_record_mac"` (error de descifrado TLS).

Un registro que falla cualquiera de estas reglas va a Quarantine con el motivo exacto en `rejection_reason`, nunca se descarta en silencio.

Seudonimización (PII):

- `wlan.bssid`, `wlan.sa`, `wlan.da`, `wlan.ra`, `wlan.ta` se protegen con HMAC-SHA256, llave gestionada en Secrets Manager, preservando el prefijo OUI (fabricante) y sustituyendo el identificador de dispositivo.
- La llave nunca viaja en el código ni en los logs — se obtiene en tiempo de ejecución desde Secrets Manager.
- **Pendiente** (ver tarea asignada en el plan de Sprint 2): la dirección de broadcast `ff:ff:ff:ff:ff:ff` se sigue seudonimizando hoy, lo cual destruye una señal relevante para detectar deauth floods. Cubierto por una prueba unitaria que falla a propósito en `glue-jobs/tests/test_pseudonymize_edge_cases.py`.

Resultados de la última corrida de perfilamiento (ver `docs/gobernanza/estado_gobernanza.md` para el detalle completo): 50.000 registros de `Deauth_0.csv`, 71,87% rechazado por la regla de `wlan.bssid` antes de calibrarla por tipo de trama. Tras la corrección, el rechazo esperado cae a prácticamente cero sobre ese mismo archivo, ya que las tramas de control dejan de exigir un campo que el protocolo no les define — pendiente de reejecutar y documentar el número real una vez ampliada la ingesta a varias familias de ataque (tarea de Sprint 2).
