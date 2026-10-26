# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Stack `infrastructure/serverless/rds/`: PostgreSQL privado y cifrado, credenciales administradas, acceso de red limitado a EMR y referencias SSM.
- Ejecutor temporal SSM sin puertos de entrada para aplicar las migraciones por la red privada; elimina sus recursos al terminar.
- Scripts de despliegue y migraciones RDS con TLS, bloqueo, checksum y registro de migraciones aplicadas.
- `TrustedOptimizationJob`: baseline sin optimizar, particiones por fecha/Label, orden por BSSID, bloom filters Parquet y validación completa de filas antes/después.
- Benchmark Athena reproducible con reutilización desactivada y evidencia de consultas, snapshots, tiempos y bytes escaneados.
### Changed
- Ingesta batch mantiene el layout optimizado de Trusted y comprueba la migración antes de escribir Raw.
- Diagrama actualizado en Python, PNG y SVG con RDS, notificaciones y distinción entre Iceberg batch y JSON streaming.

## [2.0.0] - 2026-10-26
### Added
**Capa transaccional (AWS RDS PostgreSQL — Componente 2 "BD Avanzadas" del Sprint 0)**
- `database/rds/migrations/001_schema.sql`: tablas `rule_catalog`, `alerts`, `incident_cases`, `audit_log`. Motor transaccional para alertas, casos de incidente y bitácora de auditoría, consistente con la postura CP definida en el Sprint 0.
- `database/rds/migrations/002_indexes.sql`: índice BRIN sobre `alerts.detected_at`, GIN sobre `alerts.details`, parcial sobre `incident_cases` (solo casos abiertos) y compuesto sobre `(rule_type, severity)`.
- `database/rds/migrations/003_readonly_role.sql`: roles `security_analyst` y `auditor` bajo mínimo privilegio.
- `database/rds/demo_indexes.py`: comparativo reproducible antes/después (EXPLAIN ANALYZE) de la optimización de índices.
- `infrastructure/serverless/rds/`: stack de Serverless Framework para desplegar la instancia RDS. *(Daniel — completar al desplegar)*

**Optimización de la capa analítica (Apache Iceberg)**
- Particionamiento de `trusted_awid3` por fecha del evento y `Label`, habilitando *partition pruning* en consultas de Athena. *(Daniel — completar al implementar)*
- Bloom filter / sort order sobre `wlan.bssid` para acelerar la búsqueda puntual de IoC. *(Daniel — completar al implementar)*

**Gobernanza y calidad**
- `DATA_GOVERNANCE.md`: diccionario de datos, catálogo de datos, linaje, matriz RACI y reglas de calidad consolidados en un solo documento.
- Pruebas unitarias nuevas: `glue-jobs/tests/test_schema_validation_tls.py` (rama `tls_bad_record_mac`, antes sin cubrir), `glue-jobs/tests/test_pseudonymize_edge_cases.py` (MAC no estándar, determinismo, rotación de llave, y el caso de broadcast).
- Ampliación de la ingesta batch a varias familias de ataque del dataset AWID3 (más allá de solo Deauth), para tener línea base real de tráfico normal vs. malicioso. *(Luis — completar al ejecutar)*

### Fixed
- Corrección de la regla `wlan.bssid`: ahora se condiciona al tipo de trama 802.11 (`wlan.fc.type`), recuperando las tramas de control que antes se rechazaban en masa (71,87% de la corrida del Sprint 1).
- *(Pendiente de esta entrega)* La dirección de broadcast (`ff:ff:ff:ff:ff:ff`) deja de seudonimizarse en `pseudonymize_mac_logic`, preservando la señal de tráfico difundido vs. dirigido. *(Luis — completar al corregir)*

### Changed
- `README.md`: sección "Estado del proyecto" actualizada a Sprint 2 (ver `README-seccion-sprint2.md`).

## [1.0.0] - 2026-08-31
### Added
**Data Lake & Infrastructure (IaC - Serverless Framework)**
- **Storage Stack:** Despliegue de buckets de Amazon S3 (`Raw`, `Trusted`, `Quarantine`) para la arquitectura Lakehouse.
- **Streaming Stack:** Implementación de Amazon Kinesis Data Streams y Kinesis Data Firehose para la ingesta de eventos Wi-Fi en tiempo real, con entrega automática en formato JSON al bucket `Raw`.
- **Glue Stack:** Creación del catálogo de datos (AWS Glue Data Catalog), Crawlers y Triggers automatizados.
- **Security Stack:** Configuración de AWS KMS y AWS Secrets Manager para la custodia de llaves de cifrado.

**Batch Processing (AWS Glue & Apache Iceberg)**
- `batch_ingest_job.py`: Job de PySpark (Glue 4.0) que ingesta CSVs masivos, preserva datos inmutables en la tabla `raw_awid3` y crea la tabla transaccional `trusted_awid3`.
- `compaction_job.py`: Job para el mantenimiento automatizado de tablas Iceberg (compactación de archivos y expiración de snapshots).
- **Data Quality Gate:** Lógica estricta de validación que aísla tramas incompletas (ej. sin BSSID/MAC de origen) en la capa de Cuarentena (formato Parquet).
- **Privacy by Design:** Algoritmo de ofuscación (HMAC SHA-256) aplicado a las direcciones MAC sensibles, preservando únicamente el OUI del fabricante para modelos de ML.

**Herramientas de Ingesta (`ingestion-tools`)**
- `kaggle_batch_loader.py`: Script para descarga parcial del dataset AWID3 usando la API de Kaggle y carga directa a S3 (`landing/batch/`).
- `kaggle_stream_simulator.py`: Simulador que transforma filas del CSV a JSON y las inyecta evento por evento a Kinesis Data Streams controlando la tasa de envío.
- Entorno de desarrollo local aislado mediante contenedores (Docker/DevContainers).

**Documentación**
- Diagrama de arquitectura actualizado utilizando *Diagrams as Code* (`docs/architecture.py`).
- Guía de ejecución End-to-End completa en `README.md`.

### Changed
- Configuración de roles IAM adaptada al principio de Mínimo Privilegio (Least Privilege) para cumplir con las políticas estrictas de AWS Learner Lab.
- Resolución de conflictos de nombrado de columnas en Apache Spark envolviendo referencias de columnas AWID3 con backticks y `struct(*)`.

