"""Prepare/deploy RDS and apply Cesar's migrations without logging passwords."""
import argparse
import hashlib
import json
import re
import subprocess
import urllib.request

from aws_common import ROOT, aws_environment, aws_session, parameter


def prepare(session, stage):
    ec2 = session.client('ec2')
    vpcs = ec2.describe_vpcs(Filters=[{'Name': 'is-default', 'Values': ['true']}])['Vpcs']
    if len(vpcs) != 1:
        raise RuntimeError('Expected one default VPC; configure networking explicitly.')
    vpc = vpcs[0]['VpcId']
    subnets = ec2.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [vpc]}])['Subnets']
    subnets = sorted(subnets, key=lambda s: (s['AvailabilityZone'], s['SubnetId']))
    groups = ec2.describe_security_groups(Filters=[
        {'Name': 'vpc-id', 'Values': [vpc]},
        {'Name': 'group-name', 'Values': ['ElasticMapReduce-master']},
    ])['SecurityGroups']
    if len(groups) != 1 or len({s['AvailabilityZone'] for s in subnets}) < 2:
        raise RuntimeError('EMR master group or subnets in two AZs not found.')
    rds = session.client('rds')
    # Choose a supported PostgreSQL 16 minor that can run on the requested class.
    options = []
    for page in rds.get_paginator('describe_orderable_db_instance_options').paginate(
        Engine='postgres', DBInstanceClass='db.t4g.micro', Vpc=True
    ):
        options.extend(o['EngineVersion'] for o in page['OrderableDBInstanceOptions']
                       if o['EngineVersion'].startswith('16.'))
    if not options:
        raise RuntimeError('No PostgreSQL 16 db.t4g.micro option available.')
    config = {
        'vpcId': vpc, 'subnetIds': [s['SubnetId'] for s in subnets],
        'emrMasterSecurityGroupId': groups[0]['GroupId'],
        'engineVersion': max(options, key=lambda v: tuple(map(int, v.split('.')))),
    }
    path = ROOT / '.envs' / f'rds-{stage}.json'
    path.write_text(json.dumps(config, indent=2) + '\n', encoding='utf-8')
    print(f'Local deployment configuration prepared: {path.name}')


def serverless(command, stage):
    executable = ROOT / 'infrastructure' / 'node_modules' / '.bin' / (
        'serverless.cmd' if __import__('os').name == 'nt' else 'serverless'
    )
    env = aws_environment()
    subprocess.run([str(executable), command, '--stage', stage],
                   cwd=ROOT / 'infrastructure/serverless/rds', env=env, check=True)


def migrate(session, stage):
    import psycopg
    from psycopg import sql

    secrets = session.client('secretsmanager')
    master = json.loads(secrets.get_secret_value(
        SecretId=parameter(session, stage, 'rds/MasterSecretArn'))['SecretString'])
    # Download the public AWS trust bundle; credentials never enter the filesystem.
    certificate = ROOT / '.envs' / 'rds-us-east-1-bundle.pem'
    certificate.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(
        'https://truststore.pki.rds.amazonaws.com/us-east-1/us-east-1-bundle.pem', timeout=30
    ) as response:
        certificate.write_bytes(response.read())
    with psycopg.connect(
        host=parameter(session, stage, 'rds/Endpoint'), port=5432,
        dbname='nexasecure', user=master['username'], password=master['password'],
        sslmode='verify-full', sslrootcert=str(certificate), connect_timeout=15,
    ) as connection:
        connection.execute('SELECT pg_advisory_lock(6847301)')
        connection.execute('CREATE TABLE IF NOT EXISTS schema_migrations '
                           '(filename text PRIMARY KEY, checksum text NOT NULL, '
                           'applied_at timestamptz NOT NULL DEFAULT now())')
        connection.commit()
        for path in sorted((ROOT / 'backend/database/rds/migrations').glob('*.sql')):
            original = path.read_text(encoding='utf-8')
            checksum = hashlib.sha256(original.encode()).hexdigest()
            previous = connection.execute(
                'SELECT checksum FROM schema_migrations WHERE filename = %s', (path.name,)
            ).fetchone()
            if previous:
                if previous[0] != checksum:
                    raise RuntimeError(f'Applied migration changed: {path.name}')
                print(f'Already applied: {path.name}')
                continue
            statement = re.sub(r'^\s*(BEGIN|COMMIT);\s*$', '', original, flags=re.MULTILINE)
            for role, secret_name in [('security_analyst', 'SecurityAnalystCredentials'),
                                      ('auditor', 'AuditorCredentials')]:
                placeholder = f":'{role}_password'"
                if placeholder in statement:
                    credentials = json.loads(secrets.get_secret_value(
                        SecretId=f'/nexasecure/{stage}/rds/{secret_name}')['SecretString'])
                    statement = statement.replace(
                        placeholder, sql.Literal(credentials['password']).as_string(connection))
            connection.execute(statement)
            connection.execute('INSERT INTO schema_migrations(filename, checksum) VALUES (%s, %s)',
                               (path.name, checksum))
            connection.commit()
            print(f'Applied: {path.name}')
        rows = connection.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' "
                                  'ORDER BY tablename').fetchall()
        print('Tables: ' + ', '.join(row[0] for row in rows))
        privileges = connection.execute("""
            SELECT has_table_privilege('security_analyst', 'alerts', 'SELECT'),
                   has_table_privilege('security_analyst', 'alerts', 'INSERT'),
                   has_column_privilege('security_analyst', 'incident_cases', 'status', 'UPDATE'),
                   has_table_privilege('auditor', 'audit_log', 'SELECT'),
                   has_table_privilege('auditor', 'audit_log', 'INSERT')
        """).fetchone()
        if privileges != (True, False, True, True, False):
            raise RuntimeError('Unexpected privileges after migrations')
        rule_count = connection.execute('SELECT COUNT(*) FROM rule_catalog').fetchone()[0]
        index_count = connection.execute("SELECT COUNT(*) FROM pg_indexes WHERE schemaname='public' "
                                         "AND indexname IN ('idx_alerts_detected_at_brin', "
                                         "'idx_alerts_details_gin', 'idx_incident_cases_open', "
                                         "'idx_alerts_rule_severity')").fetchone()[0]
        if rule_count < 4 or index_count != 4:
            raise RuntimeError('Missing initial rules or optimization indexes')
        print(f'Validated: {rule_count} rules, {index_count} optimization indexes and PostgreSQL role privileges.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'package', 'deploy', 'status', 'migrate'])
    parser.add_argument('--stage', default='dev')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9-]*', args.stage):
        parser.error('Invalid stage')
    session = aws_session()
    if args.command == 'prepare':
        prepare(session, args.stage)
    elif args.command in ('package', 'deploy'):
        serverless(args.command, args.stage)
    elif args.command == 'migrate':
        import psycopg
        try:
            migrate(session, args.stage)
        except psycopg.Error as error:
            # PostgreSQL error context may include SQL literals (role passwords).
            raise RuntimeError(f'Migration failed: {type(error).__name__}, '
                               f'SQLSTATE={error.sqlstate}. No credentials logged.') from None
    else:
        db = session.client('rds').describe_db_instances(
            DBInstanceIdentifier=f'nexasecure-postgres-{args.stage}')['DBInstances'][0]
        print(json.dumps({'status': db['DBInstanceStatus'], 'endpoint': db.get('Endpoint')}, indent=2))


if __name__ == '__main__':
    main()
