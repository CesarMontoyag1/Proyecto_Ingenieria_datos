#!/usr/bin/env bash
set -xe

echo "=== NexaSecure EMR Bootstrap: Instalando dependencias Python ==="

# Actualizar pip e instalar dependencias requeridas para el job
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install boto3 pandas psycopg2-binary || sudo python3 -m pip install boto3 pandas

# Intentar instalar apache-flink si está disponible en la versión de Python del sistema
sudo python3 -m pip install "apache-flink>=1.18.0" || echo "PyFlink wheel no requerido directamente para el consumidor streaming"

echo "=== Bootstrap completado exitosamente ==="

