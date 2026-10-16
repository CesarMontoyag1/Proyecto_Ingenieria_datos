"""
Simula tráfico de red WiFi en tiempo real, tomando la porción destinada
a STREAMING (segundo 50% por defecto) de categorías del dataset AWID3 y enviándola a Kinesis Data Streams.

Soporta ejecución por:
- Archivo o categoría individual: --kaggle-path "1.Deauth" o "CSV/1.Deauth/Deauth_0.csv"
- Varias categorías seleccionadas: --categories "1.Deauth" "2.Disas" "10.SQL_Injection"
- Todas las 13 categorías AWID3:  --all-categories

Uso:
    python -m src.kaggle_stream_simulator --kaggle-path "CSV/1.Deauth/Deauth_0.csv" --limit 50
    python -m src.kaggle_stream_simulator --categories "1.Deauth" "2.Disas" "10.SQL_Injection" --limit-per-cat 30
    python -m src.kaggle_stream_simulator --all-categories --limit-per-cat 20
"""

import argparse
import json
import os
import time

import boto3
import kagglehub
import pandas as pd

from src.aws_helpers import get_stream_name
from src.dataset_partitioner import (
    AWID3_ATTACK_CATEGORIES,
    get_category_file_paths,
    get_category_path,
    normalize_kaggle_path,
    partition_file_list,
    split_dataframe_rows,
)

KAGGLE_DATASET = "larbisck/awid-dataset"


def clean_record(row: pd.Series) -> dict:
    """Convierte una fila a dict JSON-serializable, reemplazando NaN por None."""
    record = row.to_dict()
    return {k: (v if pd.notna(v) else None) for k, v in record.items()}


def stream_target_data(target_path: str, stream_name: str, kinesis_client, split_ratio=0.5, split_mode="auto", limit=50, rate=10.0, dry_run=False) -> int:
    """Descarga target_path de Kaggle, aplica partición 50/50 y transmite la porción Streaming a Kinesis."""
    print(f"\n==================================================================")
    print(f"Descargando de Kaggle para Streaming: {target_path}")
    print(f"==================================================================")

    try:
        local_path = kagglehub.dataset_download(KAGGLE_DATASET, path=target_path)
    except Exception as e:
        parent_dir = os.path.dirname(target_path).replace("\\", "/")
        if target_path.lower().endswith(".csv") and parent_dir:
            print(f"Archivo específico no encontrado ({e}). Descargando carpeta contenedora: {parent_dir} ...")
            try:
                local_path = kagglehub.dataset_download(KAGGLE_DATASET, path=parent_dir)
            except Exception as e2:
                print(f"Error al descargar '{parent_dir}': {e2}")
                return 0
        else:
            print(f"Error al descargar '{target_path}': {e}")
            return 0

    files_found = []
    if os.path.isfile(local_path) and local_path.lower().endswith(".csv"):
        files_found = [local_path]
    else:
        for root, _dirs, files in os.walk(local_path):
            for f in files:
                if f.lower().endswith(".csv"):
                    files_found.append(os.path.join(root, f))

    if not files_found:
        print(f"No se encontraron archivos CSV en {local_path}")
        return 0

    files_found.sort()
    total_files = len(files_found)
    use_files_mode = (split_mode == "files") or (split_mode == "auto" and total_files >= 2)

    delay = 1.0 / rate if rate > 0 else 0
    sent = 0
    max_limit = limit if (limit and limit > 0) else float("inf")

    if use_files_mode:
        _, stream_files = partition_file_list(files_found, ratio=split_ratio)
        print(f"[Modo: Files] Consumiendo porción de Streaming ({len(stream_files)} archivo(s)):")
        for csv_path in stream_files:
            if sent >= max_limit:
                break
            for chunk in pd.read_csv(csv_path, chunksize=100):
                for _, row in chunk.iterrows():
                    if sent >= max_limit:
                        break
                    event_data = clean_record(row)
                    payload = json.dumps(event_data)

                    if dry_run:
                        if sent % 25 == 0:
                            print(f"[DRY-RUN {sent}] {payload[:100]} ...")
                    else:
                        partition_key = str(event_data.get("wlan.sa") or event_data.get("wlan.da") or "device_default")
                        response = kinesis_client.put_record(
                            StreamName=stream_name,
                            Data=payload,
                            PartitionKey=partition_key,
                        )
                        if sent % 25 == 0:
                            print(f"[{sent}] Secuencia: {response['SequenceNumber']}")

                    sent += 1
                    time.sleep(delay)
    else:
        print(f"[Modo: Rows] Consumiendo la segunda mitad ({int((1 - split_ratio) * 100)}%) de filas...")
        for csv_path in files_found:
            if sent >= max_limit:
                break
            df = pd.read_csv(csv_path)
            _, stream_df = split_dataframe_rows(df, ratio=split_ratio)
            print(f"Total filas: {len(df)} -> Streaming disponible: {len(stream_df)}")

            for _, row in stream_df.iterrows():
                if sent >= max_limit:
                    break
                event_data = clean_record(row)
                payload = json.dumps(event_data)

                if dry_run:
                    if sent % 25 == 0:
                        print(f"[DRY-RUN {sent}] {payload[:100]} ...")
                else:
                    partition_key = str(event_data.get("wlan.sa") or event_data.get("wlan.da") or "device_default")
                    response = kinesis_client.put_record(
                        StreamName=stream_name,
                        Data=payload,
                        PartitionKey=partition_key,
                    )
                    if sent % 25 == 0:
                        print(f"[{sent}] Secuencia: {response['SequenceNumber']}")

                sent += 1
                time.sleep(delay)

    print(f"Eventos transmitidos para este objetivo: {sent}")
    return sent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--kaggle-path",
        help="Subcarpeta o archivo específico del dataset (ej. '1.Deauth' o 'CSV/1.Deauth/Deauth_0.csv')."
    )
    parser.add_argument(
        "--categories", nargs="+",
        help="Lista de categorías a procesar (ej. '1.Deauth' '2.Disas' '10.SQL_Injection')."
    )
    parser.add_argument(
        "--all-categories", action="store_true",
        help="Transmite eventos de todas las 13 categorías del dataset AWID3."
    )
    parser.add_argument(
        "--split-ratio", type=float, default=0.5,
        help="Proporción asignada a Batch. El simulador consume el complemento (1 - split_ratio, default: 0.5)."
    )
    parser.add_argument(
        "--split-mode", choices=["auto", "files", "rows"], default="auto",
        help="Modo de partición: 'files', 'rows' o 'auto'."
    )
    parser.add_argument(
        "--limit-per-cat", type=int, default=50,
        help="Máximo de eventos a enviar por categoría (default: 50)."
    )
    parser.add_argument(
        "--rate", type=float, default=10.0,
        help="Eventos por segundo (default: 10)"
    )
    parser.add_argument(
        "--deploy-app", default=os.environ.get("DEPLOY_APP", "nexasecure")
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Imprime los eventos en vez de enviarlos a Kinesis."
    )
    args = parser.parse_args()

    kinesis_client = None
    stream_name = None
    if not args.dry_run:
        stream_name = get_stream_name(args.deploy_app)
        kinesis_client = boto3.client("kinesis", region_name="us-east-1")
        print(f"Transmitiendo hacia Kinesis stream: {stream_name}")
    else:
        print("Modo --dry-run activo: eventos no se transmitirán a AWS.")

    targets = []
    if args.all_categories:
        print(f"Modo: Todas las {len(AWID3_ATTACK_CATEGORIES)} categorías de ataque AWID3.")
        for cat in AWID3_ATTACK_CATEGORIES:
            targets.extend(get_category_file_paths(cat, limit_files=1))
    elif args.categories:
        print(f"Modo: {len(args.categories)} categorías seleccionadas: {args.categories}")
        for cat in args.categories:
            targets.extend(get_category_file_paths(cat, limit_files=1))
    elif args.kaggle_path:
        normalized = normalize_kaggle_path(args.kaggle_path)
        if not normalized.lower().endswith(".csv"):
            targets.extend(get_category_file_paths(normalized, limit_files=1))
        else:
            targets.append(normalized)
    else:
        parser.error("Debes especificar al menos --kaggle-path, --categories o --all-categories.")

    total_sent = 0
    for target in targets:
        sent = stream_target_data(
            target_path=target,
            stream_name=stream_name,
            kinesis_client=kinesis_client,
            split_ratio=args.split_ratio,
            split_mode=args.split_mode,
            limit=args.limit_per_cat,
            rate=args.rate,
            dry_run=args.dry_run
        )
        total_sent += sent

    print(f"\n==================================================================")
    print(f"Transmisión Streaming finalizada. Total general de eventos enviados: {total_sent}")
    print(f"==================================================================")


if __name__ == "__main__":
    main()
