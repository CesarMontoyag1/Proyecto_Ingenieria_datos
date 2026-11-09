"""Corrige las alertas de AWID3 en S3 Serving que quedaron con fecha 2020 (dt=2020-...)

debido al frame.time_epoch historico del dataset, las mueve a la particion de hoy
(dt=YYYY-MM-DD) para que Athena/QuickSight las lean dentro de projection.dt.range
(2026-01-01,NOW), y actualiza el codigo de la Lambda alertFormatter en AWS.
"""
import argparse
import io
import json
import re
import time
import zipfile
from datetime import datetime, timezone

from aws_common import ROOT, aws_session, parameter


def update_lambda_code(session, stage: str) -> None:
    lambda_client = session.client('lambda')
    fn_name = f'nexasecure-alerts-{stage}-alertFormatter'
    handler_path = ROOT / 'backend' / 'serverless' / 'alerts' / 'handler.py'

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(handler_path, arcname='handler.py')
    buf.seek(0)

    print(f"1. Actualizando código de la Lambda '{fn_name}' en AWS...")
    lambda_client.update_function_code(
        FunctionName=fn_name,
        ZipFile=buf.read(),
    )
    print("   -> Lambda actualizada exitosamente (las próximas alertas de Kaggle usarán la fecha de hoy).")


def migrate_historical_alerts(session, stage: str) -> int:
    s3 = session.client('s3')
    bucket = parameter(session, stage, 'storage/TrustedAwid3BucketName')
    prefix = 'serving/alerts/'

    print(f"\n2. Buscando alertas con fecha histórica (2020/2021) en s3://{bucket}/{prefix} ...")
    paginator = s3.get_paginator('list_objects_v2')
    moved = 0
    total_current = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.json'):
                continue
            # Ejemplo: serving/alerts/dt=2020-12-18/abc.json
            m = re.search(r'serving/alerts/dt=(\d{4}-\d{2}-\d{2})/([^/]+\.json)$', key)
            if not m:
                continue
            dt_str, filename = m.group(1), m.group(2)
            year = int(dt_str[:4])
            if year >= 2026:
                total_current += 1
                continue

            body_bytes = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
            record = json.loads(body_bytes.decode('utf-8'))

            published_at = record.get('published_at')
            if published_at:
                try:
                    detected = datetime.fromisoformat(str(published_at).replace('Z', '+00:00'))
                except ValueError:
                    detected = datetime.now(timezone.utc)
            else:
                detected = datetime.fromtimestamp(obj['LastModified'].timestamp(), tz=timezone.utc)

            new_dt = detected.strftime('%Y-%m-%d')
            record['detected_at'] = detected.isoformat()
            new_key = f'serving/alerts/dt={new_dt}/{filename}'

            s3.put_object(
                Bucket=bucket,
                Key=new_key,
                Body=(json.dumps(record, ensure_ascii=False) + '\n').encode('utf-8'),
                ContentType='application/json',
            )
            s3.delete_object(Bucket=bucket, Key=key)
            moved += 1
            total_current += 1
            print(f"   -> Movida alerta real: {key}  ==>  {new_key} (regla: {record.get('rule_type')})")

    print(f"\n3. Resumen en S3 Serving:")
    print(f"   -> Alertas reales rescatadas desde dt=2020/2021: {moved}")
    print(f"   -> Total de alertas activas hoy en Serving:     {total_current}")
    return total_current


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', default='dev')
    args = parser.parse_args()

    session = aws_session()
    update_lambda_code(session, args.stage)
    total = migrate_historical_alerts(session, args.stage)

    print("\nListo! Ahora recarga (F5) el dashboard de QuickSight en tu navegador:")
    print(f"El KPI 'Alertas totales' mostrará {total} alertas.")


if __name__ == '__main__':
    main()
