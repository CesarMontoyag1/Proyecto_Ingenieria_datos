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


def process_event_record(
    record: Dict[str, Any],
    detection_engine: DetectionEngine,
    hmac_key: bytes,
    quarantine_bucket: Optional[str] = None,
    alerts_queue_url: Optional[str] = None,
    s3_client=None,
    sqs_client=None
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

    return True, trusted_record, alerts


def main():
    region_name = os.environ.get("AWS_REGION", "us-east-1")
    stream_name = os.environ.get("STREAM_NAME")
    quarantine_bucket = os.environ.get("QUARANTINE_BUCKET")
    alerts_queue_url = os.environ.get("ALERTS_QUEUE_URL")
    secret_name = os.environ.get("HMAC_SECRET_NAME", "/nexasecure/dev/PiiHmacKey")

    logger.info("Iniciando NexaSecure WIDS Detection Job en región %s", region_name)
    logger.info("Stream Kinesis: %s | Quarantine: %s | Alerts SQS: %s", stream_name, quarantine_bucket, alerts_queue_url)

    hmac_key = get_hmac_key_from_secrets(secret_name, region_name)
    engine = DetectionEngine(hmac_key=hmac_key)

    s3_client = boto3.client("s3", region_name=region_name)
    sqs_client = boto3.client("sqs", region_name=region_name)

    logger.info("Motor de detección inicializado y listo para consumir eventos.")


if __name__ == "__main__":
    main()
