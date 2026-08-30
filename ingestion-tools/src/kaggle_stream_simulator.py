"""Simula tráfico de red WiFi en tiempo real, tomando filas de una muestra
de AWID3 descargada de Kaggle y enviándolas a Kinesis Data Streams.

El nombre del stream se lee de SSM (publicado por
infrastructure/serverless/streaming/), nunca hardcodeado.

Uso:
    python -m src.kaggle_stream_simulator --kaggle-path "Deauth" --limit 500 --rate 10
"""

import argparse
import json
import os
import time

import boto3
import kagglehub
import pandas as pd

from src.aws_helpers import get_stream_name

KAGGLE_DATASET = "larbisck/awid-dataset"
# Estructura real del dataset: CSV/<número>.<categoría>/
# Ejemplos válidos para --kaggle-path:
#   "CSV/1.Deauth"      -> tráfico de ataque Deauth
#   "CSV/1.Deauth/Deauth_0.csv"  -> un solo archivo específico
# Ver todas las categorías con: kaggle datasets files larbisck/awid-dataset


def clean_record(row: pd.Series) -> dict:
    """Convierte una fila a dict JSON-serializable, reemplazando NaN por None."""
    record = row.to_dict()
    return {k: (v if pd.notna(v) else None) for k, v in record.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--kaggle-path", required=True,
        help="Subcarpeta o archivo específico del dataset (ej. 'Deauth'). "
             "Obligatorio para evitar descargar el dataset completo."
    )
    parser.add_argument("--limit", type=int, default=500,
                         help="Máximo de eventos a enviar (default: 500)")
    parser.add_argument("--rate", type=float, default=10.0,
                         help="Eventos por segundo (default: 10)")
    parser.add_argument("--deploy-app", default=os.environ.get("DEPLOY_APP", "nexasecure"))
    parser.add_argument("--dry-run", action="store_true",
                         help="Imprime los eventos en vez de enviarlos a Kinesis "
                              "(útil para probar sin gastar nada en AWS)")
    args = parser.parse_args()

    print(f"Descargando '{args.kaggle_path}' del dataset {KAGGLE_DATASET}...")
    local_path = kagglehub.dataset_download(KAGGLE_DATASET, path=args.kaggle_path)

    csv_files = []
    if os.path.isfile(local_path) and local_path.lower().endswith(".csv"):
        # kagglehub descargó un archivo específico
        csv_files = [local_path]
    else:
        # kagglehub descargó un directorio, buscar recursivamente
        csv_files = [
            os.path.join(root, f)
            for root, _dirs, files in os.walk(local_path)
            for f in files if f.lower().endswith(".csv")
        ]
    if not csv_files:
        raise SystemExit("No se encontró ningún CSV en la ruta descargada.")

    csv_path = csv_files[0]
    print(f"Usando archivo: {csv_path}")

    kinesis_client = None
    stream_name = None
    if not args.dry_run:
        stream_name = get_stream_name(args.deploy_app)
        kinesis_client = boto3.client("kinesis", region_name="us-east-1")
        print(f"Enviando a Kinesis stream: {stream_name}")
    else:
        print("Modo --dry-run: no se enviará nada a AWS, solo se imprime en consola.")

    delay = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0

    for chunk in pd.read_csv(csv_path, chunksize=100):
        for _, row in chunk.iterrows():
            if sent >= args.limit:
                break
            event_data = clean_record(row)
            payload = json.dumps(event_data)

            if args.dry_run:
                print(payload[:200], "...")
            else:
                response = kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=payload,
                    PartitionKey=str(event_data.get("wlan.sa", "default_device")),
                )
                if sent % 50 == 0:
                    print(f"[{sent}] SequenceNumber: {response['SequenceNumber']}")

            sent += 1
            time.sleep(delay)
        if sent >= args.limit:
            break

    print(f"\nFinalizado. Total eventos enviados: {sent}")


if __name__ == "__main__":
    main()

