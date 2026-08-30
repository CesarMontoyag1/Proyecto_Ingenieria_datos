"""
Job de ingesta batch del dataset AWID3.
Autocontenido: no depende de módulos externos en src/.
Flujo:
  1. Lee CSVs desde landing/batch/ en el bucket Raw.
  2. Escribe TODO sin filtro a la tabla Iceberg de Raw (inmutabilidad).
  3. Valida cada fila -> bifurca hacia Trusted (pseudonimizado) o Quarantine.
"""
import sys
import json
import hmac
import hashlib

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, udf, struct
from pyspark.sql.types import StringType


# ─── Lógica de validación ────────────────────────────────────────────────────
CRITICAL_COLUMNS = [
    "frame.time_epoch", "frame.len", "wlan.bssid",
    "wlan.sa", "wlan.da", "Label"
]

def _validate_record_logic(row_dict: dict) -> str:
    """Devuelve '' si es válido, o el motivo de rechazo si no."""
    for col_name in CRITICAL_COLUMNS:
        val = row_dict.get(col_name)
        if val is None or str(val).strip() == "":
            return f"missing_critical_column:{col_name}"
    if "tcp.payload" in row_dict and row_dict["tcp.payload"]:
        if len(str(row_dict["tcp.payload"])) > 32767:
            return "tcp_payload_overflow"
    if (row_dict.get("tls.decryption_failed") == "1"
            or row_dict.get("tls.alert_message.desc") == "bad_record_mac"):
        return "tls_decryption_error"
    return ""


# ─── Lógica de seudonimización ───────────────────────────────────────────────
def _pseudonymize_mac(mac: str, key_bytes: bytes) -> str:
    if not mac or len(mac) < 8:
        return mac
    parts = mac.split(':')
    if len(parts) != 6:
        h = hmac.new(key_bytes, mac.encode('utf-8'), hashlib.sha256)
        return h.hexdigest()
    oui = ":".join(parts[:3])
    suffix = ":".join(parts[3:])
    h = hmac.new(key_bytes, suffix.encode('utf-8'), hashlib.sha256)
    return f"{oui}:{h.hexdigest()[:8]}"


def get_secret(secret_name: str) -> str:
    client = boto3.client('secretsmanager')
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp['SecretString'])['key']


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    # Resolver argumentos (opcionales y requeridos)
    raw_args = sys.argv
    opt_keys = [
        'JOB_NAME',
        'RAW_BUCKET_NAME',
        'TRUSTED_BUCKET_NAME',
        'QUARANTINE_BUCKET_NAME',
        'HMAC_SECRET_NAME'
    ]
    # Si viene CATALOG_DATABASE_NAME en sys.argv, incluirlo
    if any('--CATALOG_DATABASE_NAME' in arg for arg in raw_args):
        opt_keys.append('CATALOG_DATABASE_NAME')

    args = getResolvedOptions(raw_args, opt_keys)

    raw_bucket      = args['RAW_BUCKET_NAME']
    trusted_bucket  = args['TRUSTED_BUCKET_NAME']
    quar_bucket     = args['QUARANTINE_BUCKET_NAME']
    hmac_secret     = args['HMAC_SECRET_NAME']
    catalog_db      = args.get('CATALOG_DATABASE_NAME', 'nexasecure_wids_catalog_dev_local')

    # Configurar Spark con soporte de Iceberg y Glue Catalog
    conf = SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{raw_bucket}/iceberg-warehouse/")

    sc = SparkContext(conf=conf)
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args['JOB_NAME'], args)

    print(f"Usando Glue Catalog Database: {catalog_db}")

    # 1. Leer CSVs desde la zona de aterrizaje
    landing_path = f"s3://{raw_bucket}/landing/batch/"
    print(f"Leyendo CSVs desde {landing_path}")
    df = spark.read.csv(landing_path, header=True, inferSchema=True)
    total = df.count()
    print(f"Total filas leídas: {total}")

    # 2. Escribir a Raw Iceberg (inmutabilidad — SIN filtros antes de esto)
    raw_table = f"glue_catalog.{catalog_db}.raw_awid3"
    print(f"Escribiendo a tabla Iceberg Raw: {raw_table} ...")
    df.writeTo(raw_table) \
      .tableProperty("format-version", "2") \
      .tableProperty("location", f"s3://{raw_bucket}/raw/") \
      .createOrReplace()

    # 3. Validar cada fila con UDF autocontenida
    validate_udf = udf(
        lambda row: _validate_record_logic(row.asDict()),
        StringType()
    )
    # struct("*") pasa todas las columnas sin problemas de resolución por los puntos en los nombres
    df_v = df.withColumn("rejection_reason", validate_udf(struct("*")))

    df_valid   = df_v.filter(col("rejection_reason") == "")
    df_invalid = df_v.filter(col("rejection_reason") != "")

    valid_count = df_valid.count()
    invalid_count = df_invalid.count()
    print(f"Válidos: {valid_count} | Inválidos: {invalid_count}")

    # 4. Seudonimizar y escribir a Trusted Iceberg
    hmac_key = get_secret(hmac_secret).encode('utf-8')
    pseudo_udf = udf(lambda mac: _pseudonymize_mac(mac or "", hmac_key), StringType())

    mac_cols = ["wlan.bssid", "wlan.sa", "wlan.da"]
    df_trusted = df_valid.drop("rejection_reason")
    for mc in mac_cols:
        if mc in df_trusted.columns:
            # Usar backticks (`) para que Spark no confunda los puntos con campos anidados
            df_trusted = df_trusted.withColumn(mc, pseudo_udf(col(f"`{mc}`")))

    trusted_table = f"glue_catalog.{catalog_db}.trusted_awid3"
    print(f"Escribiendo a tabla Iceberg Trusted: {trusted_table} ...")
    df_trusted.writeTo(trusted_table) \
              .tableProperty("format-version", "2") \
              .tableProperty("location", f"s3://{trusted_bucket}/trusted/") \
              .createOrReplace()

    # 5. Escribir registros inválidos a Quarantine (Parquet simple)
    if invalid_count > 0:
        print(f"Escribiendo {invalid_count} registros inválidos a Quarantine...")
        df_invalid.write.parquet(
            f"s3://{quar_bucket}/quarantine/batch/",
            mode="append"
        )

    print("Job completado exitosamente.")
    job.commit()


if __name__ == "__main__":
    main()
