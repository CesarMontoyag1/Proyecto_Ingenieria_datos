#!/bin/bash
set -e

echo "=== Desplegando Glue Stack con sincronización automática de código ==="

STAGE="${1:-dev}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
DEV_NAME="${GIT_USER_NAME:-local}"
BUCKET_NAME="nexasecure-wids-awid3-raw-${STAGE}-${DEV_NAME}"

SCRIPTS_DIR="../../../glue-jobs/src"
if [ ! -d "$SCRIPTS_DIR" ]; then
    SCRIPTS_DIR="/app/glue-jobs/src"
fi

if [ -d "$SCRIPTS_DIR" ]; then
    echo "-> Sincronizando scripts de $SCRIPTS_DIR a s3://${BUCKET_NAME}/glue-jobs/ ..."
    aws s3 sync "$SCRIPTS_DIR" "s3://${BUCKET_NAME}/glue-jobs/"
else
    echo "Advertencia: No se encontró la carpeta de scripts en $SCRIPTS_DIR"
fi

# Despliegue del stack de Serverless
serverless deploy --stage "$STAGE" --region "$REGION" "${@:2}"

echo "=== Despliegue de Glue completado exitosamente ==="
