"""
Descarga porciones del dataset AWID3 desde Kaggle y sube la porción destinada
a BATCH (50% por defecto) a la zona de aterrizaje (landing) del bucket Raw.

Soporta ejecución por:
- Archivo o categoría individual: --kaggle-path "1.Deauth" o "CSV/1.Deauth/Deauth_0.csv"
- Varias categorías seleccionadas: --categories "1.Deauth" "2.Disas" "10.SQL_Injection"
- Todas las 13 categorías AWID3:  --all-categories

Uso:
    python -m src.kaggle_batch_loader --kaggle-path "CSV/1.Deauth/Deauth_0.csv" --split-mode files
    python -m src.kaggle_batch_loader --categories "1.Deauth" "2.Disas" "10.SQL_Injection" --limit-files 1
    python -m src.kaggle_batch_loader --all-categories --limit-files 1
"""

import argparse
import os
import sys
import tempfile

import boto3
import kagglehub
import pandas as pd

from src.aws_helpers import get_raw_bucket_name
from src.dataset_partitioner import (
    AWID3_ATTACK_CATEGORIES,
    get_category_file_paths,
    get_category_path,
    normalize_kaggle_path,
    partition_file_list,
    split_dataframe_rows,
)

KAGGLE_DATASET = "larbisck/awid-dataset"
LANDING_PREFIX = "landing/batch"


def is_s3_file_present(s3_client, bucket: str, key: str) -> bool:
    """Verifica si un objeto ya existe en el bucket de S3."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def upload_batch_files(target_path: str, bucket_name: str, s3_client, split_ratio=0.5, split_mode="auto", limit_files=1, force=False) -> bool:
    """Descarga target_path de Kaggle, aplica partición 50/50 y sube la porción Batch a S3."""
    base_file_name = os.path.basename(target_path)
    s3_key_standard = f"{LANDING_PREFIX}/{base_file_name}"
    s3_key_alt = f"{LANDING_PREFIX}/batch_{base_file_name}"

    if not force:
        if is_s3_file_present(s3_client, bucket_name, s3_key_standard):
            print(f"\n-> [Omitido / Ya existe en S3] s3://{bucket_name}/{s3_key_standard} (usa --force para sobreescribir)")
            return True
        if is_s3_file_present(s3_client, bucket_name, s3_key_alt):
            print(f"\n-> [Omitido / Ya existe en S3] s3://{bucket_name}/{s3_key_alt} (usa --force para sobreescribir)")
            return True

    print(f"\n==================================================================")
    print(f"Descargando de Kaggle: {target_path}")
    print(f"==================================================================")

    try:
        local_path = kagglehub.dataset_download(KAGGLE_DATASET, path=target_path)
    except Exception as e:
        print(f"Error al descargar '{target_path}': {e}", file=sys.stderr)
        return False

    files_found = []
    if os.path.isfile(local_path) and local_path.lower().endswith(".csv"):
        files_found = [local_path]
    else:
        for root, _dirs, files in os.walk(local_path):
            for f in files:
                if f.lower().endswith(".csv"):
                    files_found.append(os.path.join(root, f))

    if not files_found:
        print(f"No se encontraron archivos CSV en {local_path}", file=sys.stderr)
        return False

    files_found.sort()
    total_files = len(files_found)
    print(f"Archivos encontrados: {total_files}")

    use_files_mode = (split_mode == "files") or (split_mode == "auto" and total_files >= 2)

    if use_files_mode:
        batch_files, stream_files = partition_file_list(files_found, ratio=split_ratio)
        if limit_files:
            batch_files = batch_files[:limit_files]

        print(f"[Modo: Files] Partición {int(split_ratio * 100)}% Batch ({len(batch_files)} archivos) / {int((1 - split_ratio) * 100)}% Streaming:")
        for local_file in batch_files:
            file_name = os.path.basename(local_file)
            s3_key = f"{LANDING_PREFIX}/{file_name}"
            size_mb = os.path.getsize(local_file) / (1024 * 1024)
            print(f"-> Subiendo {file_name} ({size_mb:.1f} MB) a s3://{bucket_name}/{s3_key} ...")
            s3_client.upload_file(local_file, bucket_name, s3_key)

    else:
        target_files = files_found[:limit_files] if limit_files else files_found
        print(f"[Modo: Rows] Subiendo el primer {int(split_ratio * 100)}% de filas en {len(target_files)} archivo(s)...")

        for local_file in target_files:
            base_name = os.path.basename(local_file)
            print(f"Leyendo {base_name}...")
            df = pd.read_csv(local_file)
            total_rows = len(df)
            batch_df, _ = split_dataframe_rows(df, ratio=split_ratio)
            print(f"Filas totales: {total_rows} -> Batch: {len(batch_df)}")

            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                tmp_path = tmp.name
                batch_df.to_csv(tmp_path, index=False)

            try:
                dest_name = base_name
                s3_key = f"{LANDING_PREFIX}/{dest_name}"
                size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
                print(f"-> Subiendo partición Batch ({size_mb:.1f} MB) a s3://{bucket_name}/{s3_key} ...")
                s3_client.upload_file(tmp_path, bucket_name, s3_key)
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--kaggle-path",
        help="Subcarpeta o archivo específico del dataset (ej. '1.Deauth' o 'CSV/1.Deauth/Deauth_0.csv')."
    )
    parser.add_argument(
        "--categories", nargs="+",
        help="Lista de categorías a procesar (ej. '1.Deauth' '2.Disas' '4.Rogue_AP' '10.SQL_Injection')."
    )
    parser.add_argument(
        "--all-categories", action="store_true",
        help="Procesa automáticamente todas las 13 categorías del dataset AWID3."
    )
    parser.add_argument(
        "--split-ratio", type=float, default=0.5,
        help="Proporción asignada a Batch (default: 0.5 = 50% Batch, 50% Streaming)."
    )
    parser.add_argument(
        "--split-mode", choices=["auto", "files", "rows"], default="auto",
        help="Modo de partición: 'files', 'rows' o 'auto'."
    )
    parser.add_argument(
        "--limit-files", type=int, default=1,
        help="Máximo de archivos a subir por categoría (default: 1)."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Fuerza la descarga y sobreescritura aunque el archivo ya exista en S3 landing."
    )
    parser.add_argument(
        "--deploy-app", default=os.environ.get("DEPLOY_APP", "nexasecure")
    )
    args = parser.parse_args()

    bucket_name = get_raw_bucket_name(args.deploy_app)
    s3_client = boto3.client("s3")

    targets = []
    if args.all_categories:
        print(f"Modo: Todas las {len(AWID3_ATTACK_CATEGORIES)} categorías de ataque AWID3.")
        for cat in AWID3_ATTACK_CATEGORIES:
            targets.extend(get_category_file_paths(cat, limit_files=args.limit_files))
    elif args.categories:
        print(f"Modo: {len(args.categories)} categorías seleccionadas: {args.categories}")
        for cat in args.categories:
            targets.extend(get_category_file_paths(cat, limit_files=args.limit_files))
    elif args.kaggle_path:
        normalized = normalize_kaggle_path(args.kaggle_path)
        if not normalized.lower().endswith(".csv"):
            targets.extend(get_category_file_paths(normalized, limit_files=args.limit_files))
        else:
            targets.append(normalized)
    else:
        parser.error("Debes especificar al menos --kaggle-path, --categories o --all-categories.")

    successful = 0
    for target in targets:
        ok = upload_batch_files(
            target_path=target,
            bucket_name=bucket_name,
            s3_client=s3_client,
            split_ratio=args.split_ratio,
            split_mode=args.split_mode,
            limit_files=args.limit_files,
            force=args.force
        )
        if ok:
            successful += 1

    print(f"\n==================================================================")
    print(f"Carga Batch finalizada: {successful} de {len(targets)} archivos procesados exitosamente.")
    print("Siguiente paso: Ejecutar el Glue job 'BatchIngestJob' para procesar los datos a Iceberg.")
    print(f"==================================================================")


if __name__ == "__main__":
    main()
