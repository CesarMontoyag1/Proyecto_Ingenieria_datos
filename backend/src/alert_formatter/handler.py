import hashlib
import json
import logging
import os
import re
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REQUIRED_ALERT_FIELDS = ["rule_type", "severity", "bssid", "timestamp"]
DEFAULT_SERVING_PREFIX = "serving/alerts"
_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def format_alert_message(alert: Dict[str, Any]) -> str:
    """Formatea la alerta estructurada en un texto legible y profesional para el analista SOC."""
    rule_type = alert.get("rule_type", "UNKNOWN_RULE")
    severity = str(alert.get("severity", "MEDIUM")).upper()
    bssid = alert.get("bssid", "N/A")
    src_mac = alert.get("src_mac", "N/A")
    observed_count = alert.get("observed_count", "N/A")
    threshold = alert.get("threshold", "N/A")
    window_seconds = alert.get("window_seconds", "N/A")
    timestamp = alert.get("timestamp", "N/A")
    details = alert.get("details", {})

    lines = [
        "========================================================",
        " NEXASECURE WIDS - ALERTA DE SEGURIDAD EN TIEMPO REAL",
        "========================================================",
        f"Regla de Detección: {rule_type}",
        f"Nivel de Severidad: {severity}",
        f"BSSID (Ofuscado):   {bssid}",
        f"Origen (SRC MAC):   {src_mac}",
        f"Tramas Observadas:  {observed_count}",
        f"Umbral Configurado: {threshold}",
        f"Ventana Temporal:   {window_seconds} s",
        f"Marca Temporal:     {timestamp}",
        "--------------------------------------------------------",
        f"Detalles Adicionales: {json.dumps(details, ensure_ascii=False, indent=2)}",
        "========================================================",
        "Acción Recomendada: Inspeccionar tráfico anómalo en el SOC."
    ]
    return "\n".join(lines)


def send_to_slack_if_configured(alert: Dict[str, Any], webhook_url: Optional[str] = None):
    """Envía la alerta a un webhook de Slack si está configurado (opcional)."""
    url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return

    rule_type = alert.get("rule_type", "UNKNOWN")
    severity = str(alert.get("severity", "MEDIUM")).upper()
    bssid = alert.get("bssid", "N/A")

    slack_payload = {
        "text": f":warning: *[NexaSecure WIDS - {severity}]* Regla `{rule_type}` disparada en BSSID `{bssid}`",
        "attachments": [
            {
                "color": "#danger" if severity == "HIGH" else "#warning",
                "fields": [
                    {"title": "BSSID", "value": bssid, "short": True},
                    {"title": "Severidad", "value": severity, "short": True},
                    {"title": "Conteo", "value": str(alert.get("observed_count", "N/A")), "short": True},
                    {"title": "Umbral", "value": str(alert.get("threshold", "N/A")), "short": True},
                ]
            }
        ]
    }

    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(slack_payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            logger.info("Alerta enviada a Slack exitosamente: status=%d", resp.status)
    except Exception as e:
        logger.warning("No se pudo enviar alerta a Slack (webhook opcional): %s", e)


def _optional_int(value: Any) -> Optional[int]:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_serving_record(alert: Dict[str, Any], message_id: str) -> Dict[str, Any]:
    """
    Convierte una alerta en el registro plano de la zona Serving (tabla alerts_serving).

    Solo contiene identificadores ya seudonimizados por el detector: nunca una MAC en
    texto plano. El alert_id del detector (o, si falta, el messageId de SQS) hace que el
    registro sea idempotente: un reintento sobrescribe el mismo objeto en S3.
    """
    detected = datetime.fromtimestamp(float(alert["timestamp"]), tz=timezone.utc)
    if detected.year < 2026:
        published_at = alert.get("published_at")
        if published_at:
            try:
                detected = datetime.fromisoformat(str(published_at).replace("Z", "+00:00"))
            except ValueError:
                detected = datetime.now(timezone.utc)
        else:
            detected = datetime.now(timezone.utc)
    alert_id = str(alert.get("alert_id") or "")
    if not _SAFE_ID.match(alert_id):
        alert_id = "sqs-" + hashlib.sha256(str(alert_id or message_id).encode("utf-8")).hexdigest()[:32]
    return {
        "alert_id": alert_id,
        "rule_type": str(alert["rule_type"]),
        "severity": str(alert["severity"]).upper(),
        "bssid_pseudo": str(alert["bssid"]),
        "src_mac_pseudo": None if alert.get("src_mac") in (None, "", "N/A") else str(alert["src_mac"]),
        "observed_count": _optional_int(alert.get("observed_count")),
        "threshold": _optional_int(alert.get("threshold")),
        "window_seconds": _optional_float(alert.get("window_seconds")),
        "detected_at": detected.isoformat(),
        "detection_latency_ms": _optional_float(alert.get("detection_latency_ms")),
        "published_at": alert.get("published_at"),
        "details": json.dumps(alert.get("details", {}), ensure_ascii=False),
        "dt": detected.strftime("%Y-%m-%d"),
    }


def serving_object_key(prefix: str, record: Dict[str, Any]) -> str:
    """Ruta Hive-style particionada por fecha: <prefix>/dt=YYYY-MM-DD/<alert_id>.json"""
    return f"{prefix.strip('/')}/dt={record['dt']}/{record['alert_id']}.json"


def write_alert_to_serving(record: Dict[str, Any], s3_client, bucket: str, prefix: str) -> str:
    """Escribe el registro en S3. La columna de partición dt va en la ruta, no en el JSON."""
    body = {k: v for k, v in record.items() if k != "dt"}
    key = serving_object_key(prefix, record)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=(json.dumps(body, ensure_ascii=False) + "\n").encode("utf-8"),
        ContentType="application/json",
    )
    return key


def process_sqs_record(
    record: Dict[str, Any],
    sns_client,
    sns_topic_arn: str,
    s3_client=None,
    serving_bucket: Optional[str] = None,
    serving_prefix: str = DEFAULT_SERVING_PREFIX,
) -> None:
    """Valida un registro de SQS, lo guarda en la zona Serving (si está configurada) y lo publica en SNS."""
    body_str = record.get("body", "{}")
    alert_data = json.loads(body_str)

    # Validar campos obligatorios
    for field in REQUIRED_ALERT_FIELDS:
        if field not in alert_data or alert_data[field] is None:
            raise ValueError(f"Falta el campo obligatorio en la alerta: '{field}'")

    # Primero la persistencia: si falla, el mensaje vuelve a la cola sin haber enviado
    # el correo, y el reintento sobrescribe el mismo objeto (no hay duplicados).
    if serving_bucket and s3_client is not None:
        serving_record = build_serving_record(alert_data, record.get("messageId", "unknown"))
        key = write_alert_to_serving(serving_record, s3_client, serving_bucket, serving_prefix)
        logger.info("Alerta persistida en la zona Serving: s3://%s/%s", serving_bucket, key)

    rule_type = alert_data.get("rule_type")
    severity = str(alert_data.get("severity")).upper()
    bssid = alert_data.get("bssid")

    subject = f"[NexaSecure WIDS - {severity}] {rule_type} en {bssid}"
    if len(subject) > 100:
        subject = subject[:97] + "..."

    formatted_message = format_alert_message(alert_data)

    logger.info("Publicando alerta en SNS Topic %s: %s", sns_topic_arn, subject)
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=formatted_message,
        MessageAttributes={
            "rule_type": {"DataType": "String", "StringValue": str(rule_type)},
            "severity": {"DataType": "String", "StringValue": str(severity)},
        }
    )

    send_to_slack_if_configured(alert_data)


def lambda_handler(event: Dict[str, Any], context: Any, sns_client=None, s3_client=None) -> Dict[str, Any]:
    """
    Handler principal de la Lambda Alert Formatter con ReportBatchItemFailures.
    Lee alertas desde la cola SQS de alertas, las guarda en la zona Serving de S3
    (si SERVING_BUCKET está configurada) y las publica en SNS.
    """
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not sns_topic_arn:
        raise ValueError("Variable de entorno 'SNS_TOPIC_ARN' no configurada.")

    serving_bucket = os.environ.get("SERVING_BUCKET") or None
    serving_prefix = os.environ.get("SERVING_PREFIX") or DEFAULT_SERVING_PREFIX

    client = sns_client or boto3.client("sns")
    if serving_bucket and s3_client is None:
        s3_client = boto3.client("s3")
    batch_item_failures: List[Dict[str, str]] = []

    records = event.get("Records", [])
    logger.info("Procesando lote de %d mensajes SQS", len(records))

    for record in records:
        msg_id = record.get("messageId", "unknown")
        try:
            process_sqs_record(record, client, sns_topic_arn, s3_client, serving_bucket, serving_prefix)
        except Exception as e:
            logger.error("Error al procesar mensaje SQS %s: %s", msg_id, e, exc_info=True)
            batch_item_failures.append({"itemIdentifier": msg_id})

    return {"batchItemFailures": batch_item_failures}
