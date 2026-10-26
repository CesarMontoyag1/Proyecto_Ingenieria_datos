"""Deploy Glue resources or start the guarded one-off Trusted optimization."""
import argparse
import json
import os
import re
import subprocess

from aws_common import ROOT, aws_environment, aws_session, parameter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['package', 'deploy', 'optimize', 'status'])
    parser.add_argument('--stage', default='dev')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9-]*', args.stage):
        parser.error('Invalid stage')
    session = aws_session()
    if args.command in ('package', 'deploy'):
        bucket = parameter(session, args.stage, 'storage/RawAwid3BucketName')
        prefix = f'nexasecure-wids-awid3-raw-{args.stage}-'
        if not bucket.startswith(prefix):
            raise RuntimeError('Unexpected scripts bucket naming convention')
        env = aws_environment()
        env['GIT_USER_NAME'] = bucket[len(prefix):]
        executable = ROOT / 'infrastructure/node_modules/.bin' / (
            'serverless.cmd' if os.name == 'nt' else 'serverless')
        subprocess.run([str(executable), args.command, '--stage', args.stage],
                       cwd=ROOT / 'infrastructure/serverless/glue', env=env, check=True)
        return
    client = session.client('glue')
    optimization = parameter(session, args.stage, 'glue/TrustedOptimizationJobName')
    if args.command == 'status':
        runs = client.get_job_runs(JobName=optimization, MaxResults=1)['JobRuns']
        for run in runs:
            print(json.dumps({key: run.get(key) for key in
                             ('Id', 'JobRunState', 'ExecutionTime', 'ErrorMessage')}, indent=2))
        return
    # Check every NexaSecure writer/maintenance job in this stage before starting.
    for page in client.get_paginator('get_jobs').paginate():
        for job in page['Jobs']:
            if not job['Name'].startswith('nexasecure_') or f'_{args.stage}_' not in job['Name']:
                continue
            runs = client.get_job_runs(JobName=job['Name'])['JobRuns']
            if any(r['JobRunState'] in ('STARTING', 'RUNNING', 'STOPPING', 'WAITING') for r in runs):
                raise RuntimeError(f'Another Glue job is active: {job["Name"]}')
    emr = session.client('emr')
    for page in emr.get_paginator('list_clusters').paginate(
        ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
    ):
        if any(c['Name'] == f'nexasecure-flink-cluster-{args.stage}' for c in page['Clusters']):
            raise RuntimeError('Stop NexaSecure streaming before migration.')
    evidence = ROOT / 'docs/optimizacion/iceberg/evidencia/before.json'
    if not evidence.exists():
        raise RuntimeError('Capture the Athena before benchmark first.')
    before = json.loads(evidence.read_text(encoding='utf-8'))
    if before['stage'] != args.stage:
        raise RuntimeError('Benchmark belongs to a different stage')
    from iceberg_benchmark import metadata
    current = metadata(session, before['database'])
    if current['properties'].get('nexasecure.layout.version') == '3':
        raise RuntimeError('Trusted is already optimized; do not rewrite the benchmark dataset again.')
    try:
        baseline = client.get_table(DatabaseName=before['database'],
                                    Name='trusted_awid3_baseline')['Table']
    except client.exceptions.EntityNotFoundException:
        if current['snapshot_id'] != before['metadata']['snapshot_id']:
            raise RuntimeError('Trusted changed after the before measurement. Capture a new experiment.')
    else:
        location = baseline['Parameters']['metadata_location']
        bucket, key = location[5:].split('/', 1)
        baseline_meta = json.loads(session.client('s3').get_object(Bucket=bucket, Key=key)['Body'].read())
        original = baseline_meta['properties'].get('nexasecure.source.snapshot')
        if original != str(before['metadata']['snapshot_id']):
            raise RuntimeError('Baseline does not belong to this before measurement.')
    print('Optimization started: ' + client.start_job_run(JobName=optimization)['JobRunId'])


if __name__ == '__main__':
    main()
