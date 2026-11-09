"""Diagnostica el estado completo del pipeline de deteccion y alertas en AWS (EMR, Kinesis, SQS, Lambda, SNS, S3)."""
import argparse
import json
import re

from aws_common import aws_session, parameter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', default='dev')
    args = parser.parse_args()
    stage = args.stage
    if not re.fullmatch(r'[a-z][a-z0-9-]*', stage):
        parser.error('Invalid stage')

    session = aws_session()
    ssm = session.client('ssm')

    print("=" * 65)
    print(f"DIAGNOSTICO DEL PIPELINE DE DETECCION Y ALERTAS (Stage: {stage})")
    print("=" * 65)

    # 1. Kinesis
    print("\n[1] Kinesis Stream:")
    try:
        stream_name = parameter(session, stage, 'streaming/Awid3StreamName')
        kinesis = session.client('kinesis')
        desc = kinesis.describe_stream_summary(StreamName=stream_name)['StreamDescriptionSummary']
        print(f"    Nombre:  {stream_name}")
        print(f"    Estado:  {desc['StreamStatus']}")
        print(f"    Shards:  {desc['OpenShardCount']}")
    except Exception as e:
        print(f"    Error verificando Kinesis: {e}")

    # 2. EMR Cluster
    print("\n[2] Cluster Amazon EMR (Detector):")
    try:
        cluster_id = parameter(session, stage, 'emr/ClusterId')
        emr = session.client('emr')
        cluster = emr.describe_cluster(ClusterId=cluster_id)['Cluster']
        c_state = cluster['Status']['State']
        print(f"    Cluster ID: {cluster_id}")
        print(f"    Estado:     {c_state}")
        if c_state in ('TERMINATED', 'TERMINATING', 'TERMINATED_WITH_ERRORS'):
            print(f"    ATENCION: El cluster EMR esta {c_state}.")
            print("              Si el cluster esta terminado, nadie esta leyendo Kinesis en vivo.")
        else:
            steps = emr.list_steps(ClusterId=cluster_id)['Steps']
            for s in steps:
                print(f"    Paso '{s['Name']}': {s['Status']['State']}")
    except Exception as e:
        print(f"    No se encontro cluster EMR o parámetro SSM: {e}")

    # 3. SQS
    print("\n[3] Cola SQS (AlertsQueue):")
    try:
        queue_url = parameter(session, stage, 'queue/AlertsQueueUrl')
        sqs = session.client('sqs')
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )['Attributes']
        print(f"    URL:      {queue_url}")
        print(f"    Mensajes visibles (esperando Lambda): {attrs.get('ApproximateNumberOfMessages', '0')}")
        print(f"    Mensajes en vuelo (procesando):       {attrs.get('ApproximateNumberOfMessagesNotVisible', '0')}")
    except Exception as e:
        print(f"    Error verificando SQS: {e}")

    # 4. Lambda alertFormatter
    print("\n[4] Lambda alertFormatter:")
    try:
        lambda_client = session.client('lambda')
        fn_name = f"nexasecure-alerts-{stage}-alertFormatter"
        fn = lambda_client.get_function(FunctionName=fn_name)
        print(f"    Funcion:  {fn['Configuration']['FunctionName']}")
        print(f"    Runtime:  {fn['Configuration']['Runtime']}")
        print(f"    Estado:   {fn['Configuration']['State']}")
    except Exception as e:
        print(f"    Error verificando Lambda: {e}")

    # 5. SNS (Topico y Suscripciones)
    print("\n[5] SNS Topic y Suscripciones de Correo:")
    try:
        sns = session.client('sns')
        topic_arn = parameter(session, stage, 'notifications/SocAlertsTopicArn')
        print(f"    Topic ARN: {topic_arn}")
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)['Subscriptions']
        if not subs:
            print("    ATENCION: No hay suscripciones de correo asociadas al topico.")
        for sub in subs:
            endpoint = sub['Endpoint']
            sub_arn = sub['SubscriptionArn']
            if sub_arn == 'PendingConfirmation':
                print(f"    [PENDIENTE]: {endpoint} -> FALTA CONFIRMAR EN EL CORREO ELECTRONICO.")
            else:
                print(f"    [CONFIRMADO]: {endpoint} -> Activo ({sub_arn})")
    except Exception as e:
        print(f"    Error verificando SNS: {e}")

    # 6. S3 Serving
    print("\n[6] Alertas en Zona Serving (S3):")
    try:
        bucket = parameter(session, stage, 'storage/TrustedAwid3BucketName')
        s3 = session.client('s3')
        resp = s3.list_objects_v2(Bucket=bucket, Prefix='serving/alerts/', MaxKeys=10)
        items = resp.get('Contents', [])
        print(f"    Bucket: {bucket}")
        print(f"    Total alertas listadas (muestra): {len(items)}")
        for it in items[:5]:
            print(f"      - {it['Key']} ({it['Size']} bytes, modificado {it['LastModified']})")
    except Exception as e:
        print(f"    Error verificando S3: {e}")

    print("\n" + "=" * 65)


if __name__ == '__main__':
    main()
