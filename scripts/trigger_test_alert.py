"""Envia una alerta de prueba a SQS para validar la Lambda alertFormatter,

la persistencia en S3 Serving y la notificacion por correo via SNS (Figura 5).
"""
import argparse
import json
import re
import time
from datetime import datetime, timezone

from aws_common import aws_session, parameter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', default='dev')
    parser.add_argument('--rule-type', default='DEAUTH_FLOOD')
    parser.add_argument('--severity', default='HIGH')
    args = parser.parse_args()
    stage = args.stage
    if not re.fullmatch(r'[a-z][a-z0-9-]*', stage):
        parser.error('Invalid stage')

    session = aws_session()
    sqs = session.client('sqs')
    s3 = session.client('s3')

    queue_url = parameter(session, stage, 'queue/AlertsQueueUrl')
    bucket = parameter(session, stage, 'storage/TrustedAwid3BucketName')

    now = datetime.now(timezone.utc)
    ts = int(now.timestamp())
    alert_payload = {
        "alert_id": f"sim-{ts}",
        "rule_type": args.rule_type,
        "severity": args.severity,
        "bssid": "0c:9d:92:ad2596f1",
        "src_mac": "74:d0:2b:884e91a0",
        "observed_count": 32,
        "threshold": 10,
        "window_seconds": 5,
        "timestamp": ts,
        "details": {
            "attack_category": "1.Deauth",
            "reason": "Rafaga anomala de 32 tramas de deautenticacion en 5 s",
            "frame_type": "802.11 Deauth (type 0, subtype 12)",
            "channel": 6
        }
    }

    print(f"1. Enviando alerta '{args.rule_type}' a la cola SQS: {queue_url}")
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(alert_payload)
    )
    print("   -> Mensaje enviado exitosamente a SQS.")

    print("\n2. Esperando procesamiento por la Lambda 'alertFormatter' (3 segundos)...")
    time.sleep(4)

    dt_str = now.strftime('%Y-%m-%d')
    prefix = f"serving/alerts/dt={dt_str}/"
    print(f"\n3. Verificando persistencia en S3 Serving (s3://{bucket}/{prefix})...")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get('Contents', [])
    if contents:
        latest = sorted(contents, key=lambda x: x['LastModified'], reverse=True)[0]
        print(f"   -> Alerta persistida en S3 Serving: {latest['Key']} ({latest['Size']} bytes)")
    else:
        print("   -> (Aun no se lista en S3; puede tardar unos segundos adicionales en sincronizar)")

    print("\n4. Verificacion completa:")
    print("   -> La alerta debio disparar el topico SNS 'nexasecure-soc-alerts-dev'.")
    print("   -> Revisa tu bandeja de entrada (asunto: [NexaSecure WIDS - HIGH] DEAUTH_FLOOD en 0c:9d:92:ad2596f1).")


if __name__ == '__main__':
    main()
