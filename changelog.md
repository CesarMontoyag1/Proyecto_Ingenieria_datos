# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

