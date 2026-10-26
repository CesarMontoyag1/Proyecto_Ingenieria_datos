"""Local AWS sessions. Never print or persist credential values."""
import os
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[1]


def aws_environment():
    env = os.environ.copy()
    path = ROOT / '.envs' / 'aws.env'
    lines = path.read_text(encoding='utf-8-sig').splitlines() if path.exists() else []
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.removeprefix('export ').split('=', 1)
        env[key.strip()] = value.strip().strip('\"\'')
    env['AWS_DEFAULT_REGION'] = 'us-east-1'
    env['AWS_REGION'] = 'us-east-1'
    return env


def aws_session():
    env = aws_environment()
    return boto3.Session(
        aws_access_key_id=env.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=env.get('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=env.get('AWS_SESSION_TOKEN') or None,
        region_name='us-east-1',
    )


def parameter(session, stage, suffix):
    return session.client('ssm').get_parameter(
        Name=f'/nexasecure/{stage}/{suffix}'
    )['Parameter']['Value']
