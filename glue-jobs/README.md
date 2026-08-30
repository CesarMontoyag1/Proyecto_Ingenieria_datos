# NexaSecure WIDS - AWS Glue Batch Jobs

This subproject contains the PySpark jobs for loading the historical AWID3 dataset into the Lakehouse.

## Jobs
1. `batch_ingest_job.py`: Reads raw CSVs from the landing zone, writes completely unaltered copies to the Raw Iceberg table, and then separates valid/invalid records (valid -> pseudonymize -> Trusted Iceberg, invalid -> Quarantine).
2. `compaction_job.py`: Performs Iceberg maintenance (`rewrite_data_files` and `expire_snapshots`). Executed manually when data volume warrants it.

## Note on Code Duplication (Sprint 1)
You will notice that `awid3_schema.py` and `schema_validation.py` are duplicated here from the `flink-app/` project (adapted to PySpark UDFs). This is an intentional decision for Sprint 1. Since these run on two entirely different engines (Flink for streaming, Spark for batch), sharing a Python package adds deployment complexity that will be addressed in Sprint 2.

