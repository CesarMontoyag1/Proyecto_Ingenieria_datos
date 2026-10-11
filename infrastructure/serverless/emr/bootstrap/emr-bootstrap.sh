#!/usr/bin/env bash
set -xe

echo "=== NexaSecure EMR Bootstrap: Instalando dependencias PyFlink ==="

# Actualizar pip e instalar dependencias Python para Flink
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install apache-flink==1.18.0 boto3 pandas

# Descargar JARs para conectores de Kinesis y Apache Iceberg
JARS_DIR="/usr/lib/flink/lib"
sudo mkdir -p ${JARS_DIR}

echo "=== Bootstrap completado exitosamente ==="

