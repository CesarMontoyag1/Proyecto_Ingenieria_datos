# Historial de Cambios (Changelog)

Todos los cambios notables de este proyecto serán documentados en este archivo.

El formato está basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/), y este proyecto adhiere a [Versionado Semántico](https://semver.org/lang/es/spec/v2.0.0.html).

## [No publicado]
### Añadido
- Documentación consolidada de despliegue y pruebas de punta a punta (*end-to-end*) en `README.md`, incluyendo la preparación del entorno, el orden de los *stacks*, la carga por lotes (*batch*), la ejecución del trabajo de Glue y la simulación de *streaming*.
- Documentación de gobernanza en `DATA_GOVERNANCE.md`, con diccionario de datos, catálogo, linaje, control de acceso, reglas de calidad y matriz RACI independiente.
- *Stack* `infrastructure/serverless/rds/`: PostgreSQL privado y cifrado, credenciales administradas, acceso de red limitado a EMR y referencias en SSM.
- Ejecutor temporal de SSM sin puertos de entrada para aplicar las migraciones a través de la red privada; elimina sus recursos al finalizar.
- Scripts operativos de RDS y Glue en `scripts/`, incluyendo preparación, despliegue, estado, migraciones remotas y ejecución protegida de la optimización en la capa *Trusted*.
- Scripts de despliegue y migraciones para RDS con TLS, bloqueo asesor (*advisory lock*), suma de verificación (*checksum*) y registro de migraciones aplicadas.
- `TrustedOptimizationJob`: línea base (*baseline*) sin optimizar, particiones por fecha y etiqueta (`Label`), ordenamiento por BSSID, filtros *Bloom* de Parquet y validación completa de filas antes y después.
- Referencia comparativa (*benchmark*) de Athena reproducible con evidencia de consultas, capturas instantáneas (*snapshots*), tiempos y bytes escaneados.
- *Stack* de cola de alertas con Amazon SQS y cola de mensajes muertos (*Dead Letter Queue*), con parámetros publicados en SSM.
- *Stack* de notificaciones con Amazon SNS y un tópico para alertas del SOC, con parámetros publicados en SSM.
- Función Lambda `alertFormatter` en `backend/serverless/alerts/`, activada por SQS para validar, formatear y publicar alertas en SNS, con soporte opcional para Slack.
- *Stack* de EMR con sincronización de `flink-app` y arranque (*bootstrap*) en S3, ejecución del consumidor de detección y política de terminación automática.
- Herramientas de ingesta de Kaggle para la carga por lotes en `landing/batch/` y simulación de eventos en *streaming* hacia Kinesis.
- Pruebas unitarias para validación, seudonimización, detección, particionamiento, asistentes (*helpers*) de AWS y procesamiento de alertas.

### Cambiado
- La ingesta por lotes mantiene el diseño optimizado de la capa *Trusted* y comprueba la migración antes de escribir en la capa *Raw*.
- Diagrama actualizado en Python, PNG y SVG que incluye RDS, notificaciones y la distinción entre Iceberg por lotes y JSON en *streaming*.

## [2.0.0] - 2026-10-26
### Añadido
**Capa transaccional (AWS RDS PostgreSQL — Componente 2 "Bases de Datos Avanzadas" del Sprint 0)**
- `database/rds/migrations/001_schema.sql`: tablas `rule_catalog`, `alerts`, `incident_cases`, `audit_log`. Motor transaccional para alertas, casos de incidentes y registro de auditoría, consistente con la postura CP definida en el Sprint 0.
- `database/rds/migrations/002_indexes.sql`: índice BRIN sobre `alerts.detected_at`, GIN sobre `alerts.details`, índice parcial sobre `incident_cases` (solo casos abiertos) y compuesto sobre `(rule_type, severity)`.
- `database/rds/migrations/003_readonly_role.sql`: roles `security_analyst` y `auditor` bajo el principio de mínimo privilegio.
- `database/rds/demo_indexes.py`: comparativo reproducible antes y después (`EXPLAIN ANALYZE`) de la optimización de índices.
- `infrastructure/serverless/rds/`: *stack* del Serverless Framework para desplegar la instancia privada de RDS, su red y sus parámetros en SSM.
- `scripts/rds.py` y `scripts/rds_migrate_remote.py`: preparación de configuración, empaquetado, despliegue, estado y migración remota segura.

**Optimización de la capa analítica (Apache Iceberg)**
- Particionamiento de `trusted_awid3` por fecha del evento y etiqueta (`Label`), lo que habilita la poda de particiones (*partition pruning*) en las consultas de Athena.
- Filtro *Bloom* y ordenamiento de filas (*sort order*) sobre `wlan.bssid` para acelerar la búsqueda puntual de indicadores de compromiso (IoC).
- `trusted_awid3_baseline` para comparar el diseño anterior con el optimizado sin alterar la referencia del *benchmark*.

**Gobernanza y calidad**
- `DATA_GOVERNANCE.md`: diccionario de datos, catálogo de datos, linaje, matriz RACI y reglas de calidad consolidados en un solo documento.
- Pruebas unitarias para la rama `tls_bad_record_mac`, direcciones MAC no estándar, determinismo, rotación de llaves, validación de esquemas y reglas de detección.
- Ampliación de las herramientas de ingesta para procesar categorías y archivos específicos del conjunto de datos AWID3, además de todas las categorías disponibles.
- Roles de IAM definidos para el administrador de plataforma y el ingeniero de datos, con el alcance y permisos mínimos documentados.

### Corregido
- Corrección de la regla `wlan.bssid`: ahora se condiciona al tipo de trama 802.11 (`wlan.fc.type`), recuperando las tramas de control que antes se rechazaban de forma masiva (71,87% de la ejecución del Sprint 1).
- Corrección de la ruta documentada de la función Lambda de alertas para reflejar su ubicación real en `backend/serverless/alerts/`.

### Cambiado
- `README.md`: guía de despliegue y ejecución actualizada para conservar el flujo basado en contenedores, utilizar `serverless deploy`, desplegar todos los *stacks* requeridos y ejecutar las pruebas por lotes y en *streaming*.
- `DATA_GOVERNANCE.md`: matriz RACI actualizada para separar la responsabilidad de la aprobación; ningún rol ejecuta y aprueba la misma actividad.
- Los nombres y parámetros de la infraestructura se documentan por entorno (*stage*), región y `GIT_USER_NAME`, incluyendo la corrección del identificador de la base de datos de Glue para Spark/Iceberg.

### Limitaciones conocidas
- La dirección de difusión (*broadcast*) `ff:ff:ff:ff:ff:ff` todavía se seudonimiza en el código actual; cambiar este comportamiento requiere actualizar la lógica y sus pruebas correspondientes.
- La capa de aplicación que escribe en `audit_log` antes de realizar consultas sensibles aún no está implementada en este repositorio.

## [1.0.0] - 2026-08-31
### Añadido
**Lago de datos e infraestructura (IaC - Serverless Framework)**
- **Storage Stack:** Despliegue de los depósitos (*buckets*) de Amazon S3 (`Raw`, `Trusted`, `Quarantine`) para la arquitectura de tipo *Lakehouse*.
- **Streaming Stack:** Implementación de Amazon Kinesis Data Streams y Kinesis Data Firehose para la ingesta de eventos de Wi-Fi en tiempo real, con entrega automática en formato JSON al depósito `Raw`.
- **Glue Stack:** Creación del catálogo de datos (AWS Glue Data Catalog), rastreadores (*crawlers*) y disparadores (*triggers*) automatizados.
- **Security Stack:** Configuración de AWS KMS y AWS Secrets Manager para la custodia segura de llaves de cifrado.

**Procesamiento por lotes (AWS Glue y Apache Iceberg)**
- `batch_ingest_job.py`: trabajo de PySpark (Glue 4.0) que ingesta archivos CSV masivos, preserva datos inmutables en la tabla `raw_awid3` y crea la tabla transaccional `trusted_awid3`.
- `compaction_job.py`: trabajo destinado al mantenimiento automatizado de las tablas Iceberg (compactación de archivos y expiración de capturas instantáneas).
- **Data Quality Gate:** Lógica estricta de validación que aísla tramas incompletas (por ejemplo, aquellas sin BSSID o dirección MAC de origen) en la capa de Cuarentena (en formato Parquet).
- **Privacy by Design:** Algoritmo de ofuscación (HMAC SHA-256) aplicado a las direcciones MAC sensibles, preservando únicamente el OUI del fabricante para su uso en modelos de aprendizaje automático (*machine learning*).

**Herramientas de ingesta (`ingestion-tools`)**
- `kaggle_batch_loader.py`: script para la descarga parcial del conjunto de datos AWID3 utilizando la API de Kaggle y su carga directa a S3 (`landing/batch/`).
- `kaggle_stream_simulator.py`: simulador que transforma filas de un archivo CSV en objetos JSON y los inyecta evento por evento en Kinesis Data Streams, controlando la tasa de envío.
- Entorno de desarrollo local aislado mediante contenedores (Docker / DevContainers).

**Documentación**
- Diagrama de arquitectura actualizado utilizando la metodología de código como diagramas (*Diagrams as Code*, `docs/architecture.py`).
- Guía completa de ejecución de punta a punta (*End-to-End*) en `README.md`.

### Cambiado
- Configuración de los roles de IAM adaptada al principio de Mínimo Privilegio para cumplir con las políticas estrictas de AWS Learner Lab.
- Resolución de conflictos en los nombres de las columnas de Apache Spark mediante la inclusión de referencias con acentos graves (*backticks*) y `struct(*)` para los campos de AWID3.
