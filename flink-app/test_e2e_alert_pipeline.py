import json
import time
import boto3
from src.detection_rules import DetectionEngine
from src.detection_job import process_event_record, get_hmac_key_from_secrets

def run_e2e_test():
    ssm = boto3.client("ssm", region_name="us-east-1")
    queue_url = ssm.get_parameter(Name="/nexasecure/dev/queue/AlertsQueueUrl")["Parameter"]["Value"]
    quarantine_bucket = ssm.get_parameter(Name="/nexasecure/dev/storage/QuarantineAwid3BucketName")["Parameter"]["Value"]

    s3 = boto3.client("s3", region_name="us-east-1")
    sqs = boto3.client("sqs", region_name="us-east-1")
    hmac_key = get_hmac_key_from_secrets("/nexasecure/dev/PiiHmacKey")

    engine = DetectionEngine(hmac_key=hmac_key, deauth_threshold=5, deauth_window_sec=5.0)

    print(f"Alerts Queue: {queue_url}")
    print(f"Quarantine Bucket: {quarantine_bucket}")

    # 1. Evento inválido -> S3 Quarantine
    invalid_record = {"frame.len": 100, "Label": "Normal"}
    is_val, _, _ = process_event_record(
        invalid_record, engine, hmac_key,
        quarantine_bucket=quarantine_bucket,
        s3_client=s3
    )
    print(f"[1/3] Evento invalido enviado a Quarantine: is_valid={is_val}")

    # 2. Ráfaga de 5 tramas deauth -> SQS
    target_bssid = "00:14:6c:7e:40:80"
    attacker = "12:34:56:78:9a:bc"
    now = time.time()

    alerts_triggered = 0
    for i in range(5):
        deauth_evt = {
            "frame.time_epoch": now + (i * 0.1),
            "frame.len": 38,
            "Label": "Deauth",
            "wlan.fc.type": "0",
            "wlan.fc.subtype": "12",
            "wlan.bssid": target_bssid,
            "wlan.sa": attacker,
            "wlan.fixed.reason_code": "7"
        }
        is_val, trusted_rec, alerts = process_event_record(
            deauth_evt, engine, hmac_key,
            quarantine_bucket=quarantine_bucket,
            alerts_queue_url=queue_url,
            s3_client=s3,
            sqs_client=sqs
        )
        if alerts:
            alerts_triggered += len(alerts)
            print(f"[2/3] Alerta disparada en iteracion {i+1}: {alerts[0]['rule_type']}")

    print(f"[3/3] Total de alertas enviadas a SQS: {alerts_triggered}")
    return alerts_triggered > 0

if __name__ == "__main__":
    run_e2e_test()

