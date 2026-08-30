#!/bin/bash
set -e

# Leer nombre del stream desde SSM
STREAM_NAME=$(aws ssm get-parameter --name "/nexasecure/dev/streaming/Awid3StreamName" --query "Parameter.Value" --output text)
echo "Enviando evento de prueba a Kinesis Stream: $STREAM_NAME"

# Leer el archivo JSON original (254 columnas)
JSON_FILE="flink-app/tests/fixtures/valid_record.json"

if [ ! -f "$JSON_FILE" ]; then
    echo "Error: No se encuentra el archivo $JSON_FILE"
    exit 1
fi

echo "Leyendo payload de: $JSON_FILE"
JSON_DATA=$(cat "$JSON_FILE")

# Codificar a Base64
B64_DATA=$(echo -n "$JSON_DATA" | base64 -w 0 2>/dev/null || echo -n "$JSON_DATA" | base64)

# Enviar a Kinesis
aws kinesis put-record \
    --stream-name "$STREAM_NAME" \
    --partition-key "test-partition" \
    --data "$B64_DATA"

echo "Evento enviado exitosamente. En aproximadamente 60 segundos (buffer de Firehose), debería aparecer en tu bucket Raw S3."
