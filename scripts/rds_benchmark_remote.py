"""Ejecuta el comparativo de indices (demo_indexes.py) en RDS de forma remota via SSM.

Soluciona el bloqueo de red de RDS privado:
1. Crea un runner temporal en la VPC mediante CloudFormation/SSM (sin puertos abiertos).
2. Asegura que las migraciones de RDS esten aplicadas.
3. Siembra 50.000 alertas sinteticas en la tabla 'alerts' si esta vacia.
4. Ejecuta backend/database/rds/demo_indexes.py dentro de la VPC con acceso a RDS.
5. Captura el resultado (Seq Scan vs Index Scan con ms) y lo guarda en:
   docs/evidencias_capturas/rds/06_demo_indexes_seqscan_vs_indexscan.txt
6. Destruye la instancia y recursos temporales automaticamente.
"""
import argparse
import base64
import json
import re
import time
from pathlib import Path

from aws_common import ROOT, aws_session, parameter


def wait_stack(client, name, wanted):
    for _ in range(90):
        try:
            state = client.describe_stacks(StackName=name)['Stacks'][0]['StackStatus']
        except client.exceptions.ClientError as error:
            if wanted == 'DELETE_COMPLETE' and 'does not exist' in str(error):
                return
            raise
        if state == wanted:
            return
        if 'FAILED' in state or 'ROLLBACK' in state:
            raise RuntimeError(f'Temporary benchmark stack: {state}')
        time.sleep(10)
    raise TimeoutError('Temporary benchmark stack timed out')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', default='dev')
    args = parser.parse_args()
    stage = args.stage
    if not re.fullmatch(r'[a-z][a-z0-9-]*', stage):
        parser.error('Invalid stage')

    session = aws_session()

    # 1. Verificar si RDS esta desplegado
    rds_client = session.client('rds')
    try:
        dbs = rds_client.describe_db_instances(
            DBInstanceIdentifier=f'nexasecure-postgres-{stage}'
        )['DBInstances']
        status = dbs[0]['DBInstanceStatus']
        if status != 'available':
            raise RuntimeError(
                f"RDS 'nexasecure-postgres-{stage}' esta en estado '{status}'. Debe estar 'available'."
            )
        print(f"RDS nexasecure-postgres-{stage} verificado ('{status}').")
    except Exception as error:
        raise RuntimeError(
            f"Error al verificar RDS: {error}. Asegurate de haber ejecutado prepare y deploy de RDS."
        )

    # 2. Verificar configuracion .envs/rds-{stage}.json
    config_file = ROOT / '.envs' / f'rds-{stage}.json'
    if not config_file.exists():
        print(f'Generando {config_file.name} con prepare()...')
        from rds import prepare
        prepare(session, stage)
    config = json.loads(config_file.read_text(encoding='utf-8'))

    sts = session.client('sts').get_caller_identity()
    account = sts['Account']
    master = parameter(session, stage, 'rds/MasterSecretArn')
    db_group = parameter(session, stage, 'rds/SecurityGroupId')
    ami = session.client('ssm').get_parameter(
        Name='/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64'
    )['Parameter']['Value']

    template = {'Resources': {
        'Role': {'Type': 'AWS::IAM::Role', 'Properties': {
            'AssumeRolePolicyDocument': {'Version': '2012-10-17', 'Statement': [{
                'Effect': 'Allow', 'Principal': {'Service': 'ec2.amazonaws.com'}, 'Action': 'sts:AssumeRole'}]},
            'ManagedPolicyArns': ['arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore'],
            'Policies': [{'PolicyName': 'MigrationInputs', 'PolicyDocument': {
                'Version': '2012-10-17', 'Statement': [
                    {'Effect': 'Allow', 'Action': 'ssm:GetParameter', 'Resource': [
                        f'arn:aws:ssm:us-east-1:{account}:parameter/nexasecure/{stage}/rds/Endpoint',
                        f'arn:aws:ssm:us-east-1:{account}:parameter/nexasecure/{stage}/rds/MasterSecretArn']},
                    {'Effect': 'Allow', 'Action': 'secretsmanager:GetSecretValue', 'Resource': [master,
                        f'arn:aws:secretsmanager:us-east-1:{account}:secret:/nexasecure/{stage}/rds/SecurityAnalystCredentials-*',
                        f'arn:aws:secretsmanager:us-east-1:{account}:secret:/nexasecure/{stage}/rds/AuditorCredentials-*']},
                ]}}]}},
        'Profile': {'Type': 'AWS::IAM::InstanceProfile', 'Properties': {'Roles': [{'Ref': 'Role'}]}},
        'Group': {'Type': 'AWS::EC2::SecurityGroup', 'Properties': {
            'VpcId': config['vpcId'], 'GroupDescription': 'Temporary SSM runner for demo_indexes; no inbound ports',
            'SecurityGroupEgress': [{'IpProtocol': '-1', 'CidrIp': '0.0.0.0/0'}]}},
        'DatabaseIngress': {'Type': 'AWS::EC2::SecurityGroupIngress', 'Properties': {
            'GroupId': db_group, 'SourceSecurityGroupId': {'Ref': 'Group'},
            'IpProtocol': 'tcp', 'FromPort': 5432, 'ToPort': 5432}},
        'Instance': {'Type': 'AWS::EC2::Instance', 'Properties': {
            'ImageId': ami, 'InstanceType': 't3.micro',
            'IamInstanceProfile': {'Ref': 'Profile'},
            'MetadataOptions': {'HttpTokens': 'required'},
            'NetworkInterfaces': [{'DeviceIndex': '0', 'AssociatePublicIpAddress': True,
                'SubnetId': config['subnetIds'][0], 'GroupSet': [{'Ref': 'Group'}]}],
            'BlockDeviceMappings': [{'DeviceName': '/dev/xvda', 'Ebs': {
                'VolumeSize': 8, 'VolumeType': 'gp3', 'Encrypted': True, 'DeleteOnTermination': True}}],
            'Tags': [{'Key': 'Name', 'Value': f'nexasecure-benchmark-runner-{stage}'}],
        }},
    }}

    # Empaquetar archivos necesarios
    files = {}
    source_files = [
        ROOT / 'scripts/aws_common.py',
        ROOT / 'scripts/rds.py',
        *sorted((ROOT / 'backend/database/rds/migrations').glob('*.sql')),
        ROOT / 'backend/database/rds/demo_indexes.py',
    ]
    for path in source_files:
        files[path.relative_to(ROOT).as_posix()] = path.read_text(encoding='utf-8')
    payload = base64.b64encode(json.dumps(files).encode()).decode()

    bootstrap = (
        "import json,base64,pathlib\n"
        "root=pathlib.Path('/tmp/nexasecure-migration'); root.mkdir(exist_ok=True)\n"
        f"files=json.loads(base64.b64decode('{payload}'))\n"
        "for name,content in files.items():\n"
        " p=root/name; p.parent.mkdir(parents=True,exist_ok=True); p.write_text(content)\n"
    )

    runner_script = f"""import json, os, subprocess, sys
import boto3

stage = '{stage}'
session = boto3.Session(region_name='us-east-1')
ssm = session.client('ssm')
secrets = session.client('secretsmanager')

endpoint = ssm.get_parameter(Name=f'/nexasecure/{{stage}}/rds/Endpoint')['Parameter']['Value']
master_arn = ssm.get_parameter(Name=f'/nexasecure/{{stage}}/rds/MasterSecretArn')['Parameter']['Value']
master = json.loads(secrets.get_secret_value(SecretId=master_arn)['SecretString'])
user = master['username']
password = master['password']

print('--- PASO 1: Verificando migraciones en RDS ---', flush=True)
subprocess.run([sys.executable, '/tmp/nexasecure-migration/scripts/rds.py', 'migrate', '--stage', stage], check=True)

print('--- PASO 2: Sembrando datos en tabla alerts (50.000 filas) ---', flush=True)
import psycopg
with psycopg.connect(
    host=endpoint, port=5432, dbname='nexasecure', user=user, password=password, sslmode='require'
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM alerts")
        count = cur.fetchone()[0]
        print(f"Filas actuales en alerts: {{count}}", flush=True)
        if count < 10000:
            print("Sembrando 50.000 alertas sinteticas...", flush=True)
            seed_sql = '''
            INSERT INTO alerts (rule_type, severity, bssid_pseudo, src_mac_pseudo,
                                observed_count, threshold, window_seconds, detected_at, details)
            SELECT
              (ARRAY['DEAUTH_FLOOD','ROGUE_AP','DISAS_BURST','SSDP_FLOOD'])[1 + (i % 4)],
              (ARRAY['LOW','MEDIUM','HIGH','CRITICAL'])[1 + (i % 4)],
              '0c:9d:92:' || lpad(to_hex(i % 2000), 8, '0'),
              '74:d0:2b:' || lpad(to_hex(i % 3000), 8, '0'),
              (i % 50) + 1, 10, 5,
              now() - (i || ' seconds')::interval,
              jsonb_build_object('reason','carga sintetica para demo de indices','seq', i)
            FROM generate_series(1, 50000) AS g(i);
            '''
            cur.execute(seed_sql)
            conn.commit()
            cur.execute("SELECT count(*) FROM alerts")
            new_count = cur.fetchone()[0]
            print(f"Siembra completada. Total filas en alerts: {{new_count}}", flush=True)
        else:
            print(f"Tabla alerts ya cuenta con suficiente volumen ({{count}} filas).", flush=True)

print('--- PASO 3: Ejecutando backend/database/rds/demo_indexes.py ---', flush=True)
env = os.environ.copy()
env['NEXASECURE_RDS_DSN'] = f"postgresql://{{user}}:{{password}}@{{endpoint}}:5432/nexasecure?sslmode=require"

res = subprocess.run(
    [sys.executable, '/tmp/nexasecure-migration/backend/database/rds/demo_indexes.py'],
    env=env,
    capture_output=True,
    text=True,
    check=True
)
print(res.stdout, flush=True)
"""

    runner_b64 = base64.b64encode(runner_script.encode()).decode()

    commands = [
        'set -eu',
        'dnf -q -y install python3-pip',
        "python3 -m pip -q install 'boto3<2' 'psycopg[binary]>=3.2,<3.3' psycopg2-binary",
        "python3 - <<'PY'\n" + bootstrap + '\nPY',
        f"python3 -c \"import base64; exec(base64.b64decode('{runner_b64}').decode())\"",
    ]

    cloudformation = session.client('cloudformation')
    name = f'nexasecure-benchmark-runner-{stage}'
    created = False
    try:
        cloudformation.create_stack(
            StackName=name,
            TemplateBody=json.dumps(template),
            Capabilities=['CAPABILITY_IAM'],
            OnFailure='DELETE',
        )
        created = True
        print('Creando instancia temporal SSM en la VPC...', flush=True)
        wait_stack(cloudformation, name, 'CREATE_COMPLETE')

        resources = cloudformation.describe_stack_resources(StackName=name)['StackResources']
        instance = next(r['PhysicalResourceId'] for r in resources if r['LogicalResourceId'] == 'Instance')
        ssm = session.client('ssm')

        print(f'Esperando que la instancia {instance} registre el agente SSM...', flush=True)
        for _ in range(60):
            managed = ssm.describe_instance_information(
                Filters=[{'Key': 'InstanceIds', 'Values': [instance]}]
            )['InstanceInformationList']
            if managed and managed[0]['PingStatus'] == 'Online':
                break
            time.sleep(10)
        else:
            raise TimeoutError('El agente SSM temporal no se registro a tiempo.')

        print('Enviando comando de migracion, siembra y benchmark demo_indexes...', flush=True)
        command = ssm.send_command(
            InstanceIds=[instance],
            DocumentName='AWS-RunShellScript',
            Parameters={'commands': commands},
            TimeoutSeconds=600,
        )['Command']['CommandId']

        print('Ejecutando en VPC privada (esperando resultados)...', flush=True)
        output_captured = ""
        for _ in range(120):
            try:
                result = ssm.get_command_invocation(CommandId=command, InstanceId=instance)
            except ssm.exceptions.InvocationDoesNotExist:
                time.sleep(5)
                continue
            if result['Status'] == 'Success':
                output_captured = result['StandardOutputContent']
                print('\n' + '=' * 65)
                print('SALIDA DEL BENCHMARK RDS (FIGURA 4):')
                print('=' * 65)
                print(output_captured, flush=True)
                print('=' * 65 + '\n')
                break
            if result['Status'] in ('Failed', 'TimedOut', 'Cancelled'):
                stderr = result.get('StandardErrorContent', '')
                stdout = result.get('StandardOutputContent', '')
                raise RuntimeError(
                    f'Ejecucion remota fallo ({result["Status"]}).\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}'
                )
            time.sleep(5)
        else:
            raise TimeoutError('Tiempo de espera agotado ejecutando el benchmark en SSM.')

        # Guardar en archivo de evidencia para el informe
        out_file = ROOT / 'docs/evidencias_capturas/rds/06_demo_indexes_seqscan_vs_indexscan.txt'
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(output_captured, encoding='utf-8')
        print(f"Evidencia guardada exitosamente en:\n  {out_file.relative_to(ROOT)}")

    finally:
        if created:
            print('Eliminando instancia temporal y recursos de CloudFormation...', flush=True)
            cloudformation.delete_stack(StackName=name)
            wait_stack(cloudformation, name, 'DELETE_COMPLETE')
            print('Recursos temporales eliminados con exito.', flush=True)


if __name__ == '__main__':
    main()
