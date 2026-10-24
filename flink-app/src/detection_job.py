"""
Pipeline de Detección de Intrusiones en Streaming con Apache Flink / PyFlink.
NexaSecure WIDS - Sprint 2

Flujo por evento:
1. Validar esquema (802.11 calibrated quality rules).
2. Si es inválido: escribir en S3 Quarantine (con rejection_reason y processed_at).
3. Si es válido: seudonimizar MAC/BSSID con HMAC-SHA256 y escribir en Trusted Iceberg.
4. Evaluar reglas de detección en tiempo real (Deauth Flood, Rogue AP, Disas Burst).
5. Si dispara regla: publicar alerta en cola SQS de alertas (SLA < 2s).
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import boto3

from src.detection_rules import DetectionEngine
from src.pseudonymize import pseudonymize_mac
from src.schema_validation import validate_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("NexaSecureDetectionJob")


def get_hmac_key_from_secrets(secret_name: str, region_name: str = "us-east-1") -> bytes:
    """Lee la clave HMAC desde AWS Secrets Manager una sola vez al iniciar el job."""
    logger.info("Obteniendo clave HMAC desde Secrets Manager: %s en región %s", secret_name, region_name)
    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret_str = response.get("SecretString", "")
        try:
            secret_dict = json.loads(secret_str)
            key = secret_dict.get("hmac_key") or secret_dict.get("key") or secret_str
        except json.JSONDecodeError:
            key = secret_str
        return str(key).encode("utf-8")
    except Exception as e:
        logger.warning("No se pudo obtener el secreto desde AWS (%s). Usando clave local de respaldo para pruebas.", e)
        return b"nexasecure_local_testing_secret_key_64_bytes_padding_required_for_hmac"


def write_to_quarantine_s3(
    s3_client,
    bucket_name: str,
    record: Dict[str, Any],
    rejection_reason: str,
    processed_at: str
) -> None:
    """Escribe un registro rechazado en el bucket de cuarentena en S3 con motivo y timestamp."""
    quarantine_record = dict(record)
    quarantine_record["rejection_reason"] = rejection_reason
    quarantine_record["processed_at"] = processed_at

    key = f"quarantine_awid3/year={processed_at[:4]}/month={processed_at[5:7]}/day={processed_at[8:10]}/{int(time.time() * 1000)}_{quarantine_record.get('frame.number', 'event')}.json"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(quarantine_record).encode("utf-8"),
        ContentType="application/json"
    )


def publish_alert_to_sqs(
    sqs_client,
    queue_url: str,
    alert: Dict[str, Any],
    ingest_time: float
) -> None:
    """Publica una alerta en la cola SQS y mide la latencia de detección (SLA < 2s)."""
    current_time = time.time()
    latency_sec = current_time - ingest_time
    latency_ms = latency_sec * 1000.0

    alert["detection_latency_ms"] = round(latency_ms, 2)
    alert["published_at"] = datetime.now(timezone.utc).isoformat()

    logger.info(
        "ALERTA GENERADA [%s] en BSSID: %s | Latencia: %.2f ms (SLA objetivo < 2000 ms)",
        alert["rule_type"], alert["bssid"], latency_ms
    )

    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(alert),
        MessageAttributes={
            "rule_type": {"DataType": "String", "StringValue": alert["rule_type"]},
            "severity": {"DataType": "String", "StringValue": alert["severity"]}
        }
    )


def insert_alert_to_rds(db_conn, alert: Dict[str, Any]) -> bool:
    """
    Inserta una alerta generada en la tabla alerts de RDS PostgreSQL (001_schema.sql).
    Cumple con la postura CP y privacidad:
    - bssid_pseudo y src_mac_pseudo llegan ya seudonimizados con HMAC-SHA256.
    - RDS NUNCA recibe una dirección MAC en texto plano (Ley 1581 de 2012).
    """
    if db_conn is None:
        return False

    try:
        ts = alert.get("timestamp", time.time())
        detected_at = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        details_json = json.dumps(alert.get("details", {}))

        with db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts (
                    rule_type,
                    severity,
                    bssid_pseudo,
                    src_mac_pseudo,
                    observed_count,
                    threshold,
                    window_seconds,
                    detected_at,
                    details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    alert["rule_type"],
                    alert["severity"],
                    alert["bssid"],
                    alert.get("src_mac"),
                    int(alert.get("observed_count", 1)),
                    int(alert.get("threshold", 1)),
                    int(alert.get("window_seconds", 0)),
                    detected_at,
                    details_json,
                ),
            )
        db_conn.commit()
        logger.info(
            "Alerta [%s] registrada exitosamente en RDS PostgreSQL (bssid: %s)",
            alert["rule_type"], alert["bssid"]
        )
        return True
    except Exception as e:
        logger.error("Error al insertar alerta en RDS PostgreSQL: %s", e)
        try:
            db_conn.rollback()
        except Exception:
            pass
        return False


def get_rds_connection(dsn: Optional[str] = None):
    """
    Inicializa la conexión con RDS PostgreSQL.
    Lee DSN desde parámetro o variable de entorno NEXASECURE_RDS_DSN.
    Si psycopg2 no está disponible o la BD no es alcanzable, retorna None
    para garantizar que el procesamiento en streaming continúe hacia SQS.
    """
    dsn = dsn or os.environ.get("NEXASECURE_RDS_DSN")
    if not dsn:
        logger.info("NEXASECURE_RDS_DSN no configurado. Persistencia directa en RDS desactivada.")
        return None

    try:
        import psycopg2
        conn = psycopg2.connect(dsn)
        logger.info("Conexión exitosa con RDS PostgreSQL para persistencia de alertas.")
        return conn
    except ImportError:
        logger.warning("Librería psycopg2 no instalada. No se pudo conectar a RDS.")
        return None
    except Exception as e:
        logger.warning("No se pudo conectar a RDS PostgreSQL (%s). El streaming continuará hacia SQS.", e)
        return None


def process_event_record(
    record: Dict[str, Any],
    detection_engine: DetectionEngine,
    hmac_key: bytes,
    quarantine_bucket: Optional[str] = None,
    alerts_queue_url: Optional[str] = None,
    s3_client=None,
    sqs_client=None,
    db_conn=None
) -> Tuple[bool, Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Función pura/desacoplada que procesa un evento individual:
    1. Valida calidad
    2. Si inválido -> registra motivo de rechazo
    3. Si válido -> seudonimiza MACs y evalúa reglas de detección
    Retorna: (is_valid, record_transformado, lista_alertas)
    """
    now_utc = datetime.now(timezone.utc).isoformat()
    raw_event_epoch = record.get("frame.time_epoch")
    try:
        event_time = float(raw_event_epoch) if raw_event_epoch is not None else time.time()
    except (ValueError, TypeError):
        event_time = time.time()

    # 1. Validación de Calidad
    validation = validate_record(record)
    if not validation.is_valid:
        if s3_client and quarantine_bucket:
            try:
                write_to_quarantine_s3(s3_client, quarantine_bucket, record, validation.rejection_reason or "unknown", now_utc)
            except Exception as e:
                logger.error("Error al escribir en Quarantine S3: %s", e)
        return False, None, []

    # 2. Seudonimización de direcciones MAC
    trusted_record = dict(record)
    for mac_col in ["wlan.bssid", "wlan.sa", "wlan.da", "wlan.ra", "wlan.ta"]:
        if mac_col in trusted_record and trusted_record[mac_col]:
            trusted_record[mac_col] = pseudonymize_mac(str(trusted_record[mac_col]), hmac_key)

    # 3. Evaluación de Reglas de Detección
    alerts = detection_engine.process_event(record)
    for alert in alerts:
        if sqs_client and alerts_queue_url:
            try:
                publish_alert_to_sqs(sqs_client, alerts_queue_url, alert, event_time)
            except Exception as e:
                logger.error("Error al publicar alerta en SQS: %s", e)
        if db_conn:
            try:
                insert_alert_to_rds(db_conn, alert)
            except Exception as e:
                logger.error("Error al insertar alerta en RDS: %s", e)

    return True, trusted_record, alerts


def run_kinesis_consumer(
    stream_name: str,
    detection_engine: DetectionEngine,
    hmac_key: bytes,
    quarantine_bucket: str,
    alerts_queue_url: str,
    region_name: str = "us-east-1",
    poll_interval: float = 1.0,
    iterator_type: str = "LATEST",
    db_conn=None
):
    """
    Bucle consumidor en tiempo real desde Kinesis Data Streams.
    Procesa cada evento por el pipeline de validación y detección con SLA < 2s.
    """
    kinesis_client = boto3.client("kinesis", region_name=region_name)
    s3_client = boto3.client("s3", region_name=region_name)
    sqs_client = boto3.client("sqs", region_name=region_name)

    logger.info("Obteniendo shards del stream Kinesis: %s", stream_name)
    desc = kinesis_client.describe_stream(StreamName=stream_name)
    shards = desc["StreamDescription"]["Shards"]
    logger.info("Shards activos encontrados: %d", len(shards))

    shard_iterators = {}
    for s in shards:
        shard_id = s["ShardId"]
        iter_resp = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType=iterator_type
        )
        shard_iterators[shard_id] = iter_resp["ShardIterator"]

    logger.info("Consumidor activo y escuchando eventos en tiempo real. (Presiona Ctrl+C para detener)")
    total_processed = 0

    try:
        while True:
            had_records = False
            for shard_id, shard_iter in list(shard_iterators.items()):
                if not shard_iter:
                    continue
                records_resp = kinesis_client.get_records(ShardIterator=shard_iter, Limit=100)
                shard_iterators[shard_id] = records_resp.get("NextShardIterator")

                records = records_resp.get("Records", [])
                if records:
                    had_records = True
                    for r in records:
                        total_processed += 1
                        data_bytes = r.get("Data", b"{}")
                        try:
                            record_dict = json.loads(data_bytes.decode("utf-8"))
                            process_event_record(
                                record=record_dict,
                                detection_engine=detection_engine,
                                hmac_key=hmac_key,
                                quarantine_bucket=quarantine_bucket,
                                alerts_queue_url=alerts_queue_url,
                                s3_client=s3_client,
                                sqs_client=sqs_client,
                                db_conn=db_conn
                            )
                        except Exception as e:
                            logger.error("Error al procesar registro de Kinesis: %s", e)

            # Pequeña pausa para no saturar la API de Kinesis si no hay registros
            if not had_records:
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Consumidor detenido por el usuario. Total eventos procesados: %d", total_processed)


def resolve_ssm_parameter(ssm_client, param_name: str, default: Optional[str] = None) -> Optional[str]:
    """Obtiene un parámetro de SSM Parameter Store si existe."""
    try:
        resp = ssm_client.get_parameter(Name=param_name)
        return resp["Parameter"]["Value"]
    except Exception:
        return default


def main():
    region_name = os.environ.get("AWS_REGION", "us-east-1")
    stage = os.environ.get("STAGE", "dev")

    ssm = boto3.client("ssm", region_name=region_name)

    # Resolver nombres automáticamente desde SSM si no están en variables de entorno
    stream_name = os.environ.get(
        "STREAM_NAME",
        resolve_ssm_parameter(ssm, f"/nexasecure/{stage}/streaming/Awid3StreamName")
        or resolve_ssm_parameter(ssm, f"/nexasecure/{stage}/stream/Awid3StreamName")
    )
    quarantine_bucket = os.environ.get(
        "QUARANTINE_BUCKET",
        resolve_ssm_parameter(ssm, f"/nexasecure/{stage}/storage/QuarantineAwid3BucketName")
    )
    alerts_queue_url = os.environ.get(
        "ALERTS_QUEUE_URL",
        resolve_ssm_parameter(ssm, f"/nexasecure/{stage}/queue/AlertsQueueUrl")
    )
    secret_name = os.environ.get("HMAC_SECRET_NAME", f"/nexasecure/{stage}/PiiHmacKey")

    logger.info("=== NexaSecure WIDS Detection Job (Streaming) ===")
    logger.info("Región AWS:        %s", region_name)
    logger.info("Stream Kinesis:    %s", stream_name)
    logger.info("Bucket Quarantine: %s", quarantine_bucket)
    logger.info("Cola SQS Alertas:  %s", alerts_queue_url)

    if not stream_name:
        logger.error("Error: No se encontró el nombre del Kinesis Stream en SSM ni en STREAM_NAME.")
        sys.exit(1)

    hmac_key = get_hmac_key_from_secrets(secret_name, region_name)
    engine = DetectionEngine(hmac_key=hmac_key)

    # Conectar opcionalmente a RDS PostgreSQL e intentar recalibrar reglas
    db_conn = get_rds_connection()
    if db_conn:
        engine.load_rules_from_rds(db_conn)

    iterator_type = os.environ.get("ITERATOR_TYPE", "LATEST")
    run_kinesis_consumer(
        stream_name=stream_name,
        detection_engine=engine,
        hmac_key=hmac_key,
        quarantine_bucket=quarantine_bucket,
        alerts_queue_url=alerts_queue_url,
        region_name=region_name,
        iterator_type=iterator_type,
        db_conn=db_conn
    )


if __name__ == "__main__":
    main()
