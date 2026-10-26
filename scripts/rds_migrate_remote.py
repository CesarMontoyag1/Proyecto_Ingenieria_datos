"""Apply migrations through a disposable SSM instance when local TCP is blocked.

No SSH ports, keys or embedded AWS credentials. The temporary CloudFormation
stack grants only migration-secret/parameter access and deletes itself in finally.
"""
import argparse
import base64
import json
import re
import time

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
            raise RuntimeError(f'Temporary migration stack: {state}')
        time.sleep(10)
    raise TimeoutError('Temporary migration stack timed out')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', default='dev')
    args = parser.parse_args()
    stage = args.stage
    if not re.fullmatch(r'[a-z][a-z0-9-]*', stage):
        parser.error('Invalid stage')
    session = aws_session()
    sts = session.client('sts').get_caller_identity()
    account = sts['Account']
    config = json.loads((ROOT / '.envs' / f'rds-{stage}.json').read_text())
    master = parameter(session, stage, 'rds/MasterSecretArn')
    db_group = parameter(session, stage, 'rds/SecurityGroupId')
    ami = session.client('ssm').get_parameter(
        Name='/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64')['Parameter']['Value']
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
            'VpcId': config['vpcId'], 'GroupDescription': 'Temporary SSM migration runner; no inbound ports',
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
            'Tags': [{'Key': 'Name', 'Value': 'nexasecure-temporary-migration'}],
        }},
    }}
    files = {}
    for path in [ROOT / 'scripts/aws_common.py', ROOT / 'scripts/rds.py',
                 *sorted((ROOT / 'backend/database/rds/migrations').glob('*.sql'))]:
        files[path.relative_to(ROOT).as_posix()] = path.read_text(encoding='utf-8')
    payload = base64.b64encode(json.dumps(files).encode()).decode()
    bootstrap = (
        "import json,base64,pathlib\n"
        "root=pathlib.Path('/tmp/nexasecure-migration'); root.mkdir(exist_ok=True)\n"
        f"files=json.loads(base64.b64decode('{payload}'))\n"
        "for name,content in files.items():\n"
        " p=root/name; p.parent.mkdir(parents=True,exist_ok=True); p.write_text(content)\n"
    )
    commands = ['set -eu', 'dnf -q -y install python3-pip',
                "python3 -m pip -q install 'boto3<2' 'psycopg[binary]>=3.2,<3.3'",
                "python3 - <<'PY'\n" + bootstrap + '\nPY',
                f'python3 /tmp/nexasecure-migration/scripts/rds.py migrate --stage {stage}']
    cloudformation = session.client('cloudformation')
    name = f'nexasecure-migration-runner-{stage}'
    created = False
    try:
        cloudformation.create_stack(StackName=name, TemplateBody=json.dumps(template),
                                    Capabilities=['CAPABILITY_IAM'], OnFailure='DELETE')
        created = True
        print('Creating temporary SSM migration runner (no inbound ports).', flush=True)
        wait_stack(cloudformation, name, 'CREATE_COMPLETE')
        resources = cloudformation.describe_stack_resources(StackName=name)['StackResources']
        instance = next(r['PhysicalResourceId'] for r in resources if r['LogicalResourceId'] == 'Instance')
        ssm = session.client('ssm')
        for _ in range(60):
            managed = ssm.describe_instance_information(Filters=[{'Key': 'InstanceIds', 'Values': [instance]}])['InstanceInformationList']
            if managed and managed[0]['PingStatus'] == 'Online':
                break
            time.sleep(10)
        else:
            raise TimeoutError('Temporary SSM agent did not register')
        command = ssm.send_command(InstanceIds=[instance], DocumentName='AWS-RunShellScript',
                                   Parameters={'commands': commands}, TimeoutSeconds=600)['Command']['CommandId']
        print('Applying original migrations over the private VPC connection.', flush=True)
        for _ in range(120):
            try:
                result = ssm.get_command_invocation(CommandId=command, InstanceId=instance)
            except ssm.exceptions.InvocationDoesNotExist:
                time.sleep(5)
                continue
            if result['Status'] == 'Success':
                # Only migration verification output, never SQL/secrets or install stderr.
                print(result['StandardOutputContent'], flush=True)
                break
            if result['Status'] in ('Failed', 'TimedOut', 'Cancelled'):
                raise RuntimeError(f'Remote migration failed ({result["Status"]}); command ID: {command}')
            time.sleep(5)
        else:
            raise TimeoutError('Remote migrations timed out')
    finally:
        if created:
            cloudformation.delete_stack(StackName=name)
            print('Removing temporary instance, IAM profile/role and database ingress.', flush=True)
            wait_stack(cloudformation, name, 'DELETE_COMPLETE')
            print('Temporary migration resources removed.', flush=True)


if __name__ == '__main__':
    main()
