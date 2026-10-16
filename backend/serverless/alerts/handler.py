import json
import logging
import os
import urllib.request
from typing import Any, Dict, List, Optional
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REQUIRED_ALERT_FIELDS = ["rule_type", "severity", "bssid", "timestamp"]


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


def process_sqs_record(record: Dict[str, Any], sns_client, sns_topic_arn: str) -> None:
    """Valida y procesa un registro individual de SQS hacia SNS."""
    body_str = record.get("body", "{}")
    alert_data = json.loads(body_str)

    # Validar campos obligatorios
    for field in REQUIRED_ALERT_FIELDS:
        if field not in alert_data or alert_data[field] is None:
            raise ValueError(f"Falta el campo obligatorio en la alerta: '{field}'")

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


def lambda_handler(event: Dict[str, Any], context: Any, sns_client=None) -> Dict[str, Any]:
    """
    Handler principal de la Lambda Alert Formatter con ReportBatchItemFailures.
    Lee alertas desde la cola SQS de alertas y las publica en SNS.
    """
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not sns_topic_arn:
        raise ValueError("Variable de entorno 'SNS_TOPIC_ARN' no configurada.")

    client = sns_client or boto3.client("sns")
    batch_item_failures: List[Dict[str, str]] = []

    records = event.get("Records", [])
    logger.info("Procesando lote de %d mensajes SQS", len(records))

    for record in records:
        msg_id = record.get("messageId", "unknown")
        try:
            process_sqs_record(record, client, sns_topic_arn)
        except Exception as e:
            logger.error("Error al procesar mensaje SQS %s: %s", msg_id, e, exc_info=True)
            batch_item_failures.append({"itemIdentifier": msg_id})

    return {"batchItemFailures": batch_item_failures}

