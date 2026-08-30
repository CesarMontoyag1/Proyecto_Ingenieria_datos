"""Descarga una porción del dataset AWID3 desde Kaggle (sin bajar los 46GB
completos) y la sube a la zona de aterrizaje (landing) del bucket Raw, para
que el job de Glue (glue-jobs/src/batch_ingest_job.py) la procese después.

IMPORTANTE: este script NO escribe en la tabla Iceberg de Raw. Escribe en
un prefijo `landing/batch/` de CSVs crudos. La tabla Iceberg de Raw solo
la escribe Glue, con job-bookmark habilitado para no reprocesar archivos.

Uso:
    python -m src.kaggle_batch_loader --kaggle-path "Normal_Traffic" --limit-files 2
"""

import argparse
import os
import sys

import boto3
import kagglehub

from src.aws_helpers import get_raw_bucket_name

KAGGLE_DATASET = "larbisck/awid-dataset"
# Estructura real del dataset: CSV/<número>.<categoría>/
# Ejemplos válidos para --kaggle-path:
#   "CSV/1.Deauth"                    -> todos los archivos de Deauth (~1.3GB)
#   "CSV/1.Deauth/Deauth_0.csv"       -> un solo archivo (~37MB)
# Ver todas las categorías con: kaggle datasets files larbisck/awid-dataset
LANDING_PREFIX = "landing/batch"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--kaggle-path", required=True,
        help="Subcarpeta o archivo específico dentro del dataset de Kaggle "
             "(ej. 'Normal_Traffic' o 'Deauth'). NUNCA dejar vacío: sin esto "
             "kagglehub puede intentar bajar el dataset completo (46GB)."
    )
    parser.add_argument(
        "--limit-files", type=int, default=2,
        help="Máximo de archivos a subir en esta corrida (default: 2, para "
             "no agotar el crédito de AWS Academy ni el disco local)."
    )
    parser.add_argument(
        "--deploy-app", default=os.environ.get("DEPLOY_APP", "nexasecure")
    )
    args = parser.parse_args()

    print(f"Descargando '{args.kaggle_path}' del dataset {KAGGLE_DATASET}...")
    local_path = kagglehub.dataset_download(KAGGLE_DATASET, path=args.kaggle_path)
    print(f"Descarga completa en: {local_path}")

    files_found = []
    if os.path.isfile(local_path) and local_path.lower().endswith(".csv"):
        # kagglehub descargó un archivo específico
        files_found = [local_path]
    else:
        # kagglehub descargó un directorio, buscar recursivamente
        for root, _dirs, files in os.walk(local_path):
            for f in files:
                if f.lower().endswith(".csv"):
                    files_found.append(os.path.join(root, f))

    if not files_found:
        print("No se encontraron archivos CSV en la ruta descargada.", file=sys.stderr)
        sys.exit(1)

    files_to_upload = files_found[: args.limit_files]
    print(f"Se subirán {len(files_to_upload)} de {len(files_found)} archivos encontrados.")

    bucket_name = get_raw_bucket_name(args.deploy_app)
    s3_client = boto3.client("s3")

    for local_file_path in files_to_upload:
        file_name = os.path.basename(local_file_path)
        s3_key = f"{LANDING_PREFIX}/{file_name}"
        size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
        print(f"Subiendo {file_name} ({size_mb:.1f} MB) a "
              f"s3://{bucket_name}/{s3_key} ...")
        s3_client.upload_file(local_file_path, bucket_name, s3_key)

    print("\nCarga a landing/batch/ completada.")
    print("Siguiente paso: correr el Glue job 'BatchIngestJob' para "
          "procesar estos archivos hacia la tabla Iceberg de Raw/Trusted.")


if __name__ == "__main__":
    main()

