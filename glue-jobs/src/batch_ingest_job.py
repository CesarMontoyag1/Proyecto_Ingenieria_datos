"""
Job de ingesta batch del dataset AWID3.
El layout Trusted se entrega mediante iceberg_layout.py (--extra-py-files).
Flujo:
  1. Lee CSVs incrementalmente desde landing/batch/ usando AWS Glue Job Bookmarks.
  2. Escribe TODO sin filtro a la tabla Iceberg de Raw (inmutabilidad).
  3. Valida cada fila según protocolo Wi-Fi 802.11 -> bifurca hacia Trusted (pseudonimizado) o Quarantine.
  4. Realiza append a las tablas Iceberg de Raw y Trusted.
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
from iceberg_layout import assert_migrated, save_trusted, table_identifier


# ─── Lógica de validación (Calibrada para 802.11) ─────────────────────────────
UNIVERSAL_CRITICAL_COLUMNS = [
    "frame.time_epoch", "frame.len", "Label"
]

def _clean_str(val):
    if val is None:
        return ""
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s

def _validate_record_logic(row_dict: dict) -> str:
    """
    Devuelve '' si es válido, o el motivo de rechazo si no.
    Calibrado para el protocolo 802.11: Tramas de Control (type 1) no tienen BSSID por diseño.
    """
    # 1. Validar columnas universales presentes en cualquier trama
    for col_name in UNIVERSAL_CRITICAL_COLUMNS:
        val = row_dict.get(col_name)
        if val is None or _clean_str(val) == "":
            return f"missing_critical_column:{col_name}"

    # 2. Validación condicional según tipo de trama 802.11 (wlan.fc.type)
    # 0 = Management (Gestión), 1 = Control, 2 = Data (Datos)
    frame_type = _clean_str(row_dict.get("wlan.fc.type"))

    if frame_type == "1":
        # Tramas de Control (ACK, CTS, RTS): No tienen wlan.bssid por estándar 802.11.
        # Deben tener al menos la dirección del receptor (wlan.ra o wlan.da)
        ra = _clean_str(row_dict.get("wlan.ra"))
        da = _clean_str(row_dict.get("wlan.da"))
        if not (ra or da):
            return "missing_receiver_address_in_control_frame"
    else:
        # Tramas de Gestión (0) y Datos (2): requieren BSSID y dirección de origen
        bssid = _clean_str(row_dict.get("wlan.bssid"))
        sa = _clean_str(row_dict.get("wlan.sa"))
        ta = _clean_str(row_dict.get("wlan.ta"))
        if not bssid:
            return "missing_bssid_in_data_or_mgmt_frame"
        if not (sa or ta):
            return "missing_source_address"

    # 3. Validar desbordamiento de payload TCP
    if "tcp.payload" in row_dict and row_dict["tcp.payload"]:
        if len(str(row_dict["tcp.payload"])) > 32767:
            return "tcp_payload_overflow"

    # 4. Validar descifrado TLS si hay alerta de bad record mac
    if row_dict.get("tls.alert_message.desc") == "bad_record_mac":
        return "tls_bad_record_mac"

    return ""


# ─── Lógica de seudonimización ───────────────────────────────────────────────
def _pseudonymize_mac(mac: str, key_bytes: bytes) -> str:
    if not mac or len(str(mac).strip()) < 8:
        return mac
    mac_str = str(mac).strip()
    parts = mac_str.split(':')
    if len(parts) != 6:
        h = hmac.new(key_bytes, mac_str.encode('utf-8'), hashlib.sha256)
        return h.hexdigest()
    oui = ":".join(parts[:3])
    suffix = ":".join(parts[3:])
    h = hmac.new(key_bytes, suffix.encode('utf-8'), hashlib.sha256)
    return f"{oui}:{h.hexdigest()[:8]}"


def get_secret(secret_name: str) -> str:
    client = boto3.client('secretsmanager')
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp['SecretString'])['key']


def save_iceberg_table(df_to_save, table_name: str, location_s3: str, spark_session):
    """
    Crea la tabla Iceberg si no existe, o hace append si ya existe (idempotencia y bookmarking).
    """
    table_exists = False
    try:
        spark_session.sql(f"DESCRIBE TABLE {table_name}")
        table_exists = True
    except Exception as e:
        print(f"Tabla {table_name} no existe aún ({e}). Se procederá a crearla.")
        table_exists = False

    if table_exists:
        print(f"Haciendo append a tabla Iceberg existente: {table_name} ...")
        df_to_save.writeTo(table_name).append()
    else:
        print(f"Creando tabla Iceberg inicial: {table_name} en {location_s3} ...")
        df_to_save.writeTo(table_name) \
                  .tableProperty("format-version", "2") \
                  .tableProperty("location", location_s3) \
                  .create()


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    raw_args = sys.argv
    opt_keys = [
        'JOB_NAME',
        'RAW_BUCKET_NAME',
        'TRUSTED_BUCKET_NAME',
        'QUARANTINE_BUCKET_NAME',
        'HMAC_SECRET_NAME'
    ]
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
    conf.set("spark.sql.session.timeZone", "UTC")

    sc = SparkContext(conf=conf)
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args['JOB_NAME'], args)

    trusted_table = table_identifier(catalog_db, 'trusted_awid3')
    # Fail before touching Raw/bookmarks if the existing Trusted needs migration.
    assert_migrated(spark, trusted_table)

    print(f"Usando Glue Catalog Database: {catalog_db}")

    # 1. Leer CSVs incrementalmente desde landing/batch/ usando AWS Glue Job Bookmarks
    landing_path = f"s3://{raw_bucket}/landing/batch/"
    print(f"Leyendo CSVs incrementalmente con Job Bookmark desde {landing_path}")

    dynamic_frame = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [landing_path],
            "recurse": True
        },
        format="csv",
        format_options={
            "withHeader": True,
            "separator": ","
        },
        transformation_ctx="awid3_csv_source"
    )

    df = dynamic_frame.toDF()
    total = df.count()
    print(f"Total filas leídas en este lote: {total}")

    if total == 0:
        print("No se encontraron registros nuevos en landing/batch/ (Job Bookmark al día). Finalizando.")
        job.commit()
        return

    # 2. Escribir a Raw Iceberg (inmutabilidad — SIN filtros antes de esto)
    raw_table = f"glue_catalog.{catalog_db}.raw_awid3"
    save_iceberg_table(df, raw_table, f"s3://{raw_bucket}/raw/", spark)

    # 3. Validar cada fila con UDF calibrada para 802.11
    validate_udf = udf(
        lambda row: _validate_record_logic(row.asDict()),
        StringType()
    )
    df_v = df.withColumn("rejection_reason", validate_udf(struct("*")))

    df_valid   = df_v.filter(col("rejection_reason") == "")
    df_invalid = df_v.filter(col("rejection_reason") != "")

    valid_count = df_valid.count()
    invalid_count = df_invalid.count()
    print(f"Válidos: {valid_count} ({valid_count*100.0/total:.2f}%) | Inválidos: {invalid_count} ({invalid_count*100.0/total:.2f}%)")

    # 4. Seudonimizar y escribir a Trusted Iceberg
    hmac_key = get_secret(hmac_secret).encode('utf-8')
    pseudo_udf = udf(lambda mac: _pseudonymize_mac(mac, hmac_key) if mac else mac, StringType())

    mac_cols = ["wlan.bssid", "wlan.sa", "wlan.da", "wlan.ra", "wlan.ta"]
    df_trusted = df_valid.drop("rejection_reason")
    for mc in mac_cols:
        if mc in df_trusted.columns:
            df_trusted = df_trusted.withColumn(mc, pseudo_udf(col(f"`{mc}`")))

    save_trusted(df_trusted, trusted_table, f"s3://{trusted_bucket}/trusted/", spark)

    # 5. Escribir registros inválidos a Quarantine (Parquet)
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
