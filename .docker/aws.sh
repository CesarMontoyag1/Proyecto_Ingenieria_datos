#!/bin/bash
# aws.sh: Configura AWS CLI
if [ -f "/app/.envs/aws.env" ]; then
    export $(grep -v '^#' /app/.envs/aws.env | xargs)
    mkdir -p ~/.aws
    cat <<EOF > ~/.aws/credentials
[default]
aws_access_key_id = $AWS_ACCESS_KEY_ID
aws_secret_access_key = $AWS_SECRET_ACCESS_KEY
aws_session_token = $AWS_SESSION_TOKEN
EOF
    cat <<EOF > ~/.aws/config
[default]
region = ${AWS_DEFAULT_REGION:-us-east-1}
output = ${AWS_DEFAULT_OUTPUT:-json}
EOF
    echo "AWS credentials configuradas."
else
    echo "No se encontro /app/.envs/aws.env"
fi
