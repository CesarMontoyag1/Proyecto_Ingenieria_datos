"""
Módulo para la partición y balanceo del dataset AWID3 (50% Batch / 50% Streaming)
por cada categoría de ataque.
"""
import os
from typing import List, Tuple
import pandas as pd

AWID3_ATTACK_CATEGORIES = [
    "1.Deauth",
    "2.Disas",
    "3.(Re)Assoc",
    "4.Rogue_AP",
    "5.Krack",
    "6.Kr00k",
    "7.SSH",
    "8.Botnet",
    "9.Malware",
    "10.SQL_Injection",
    "11.SSDP",
    "12.Evil_Twin",
    "13.Website_spoofing"
]

CATEGORY_FILE_PREFIX = {
    "1.Deauth": "Deauth",
    "2.Disas": "Disas",
    "3.(Re)Assoc": "(Re)Assoc",
    "4.Rogue_AP": "RogueAP",
    "5.Krack": "Krack",
    "6.Kr00k": "Kr00k",
    "7.SSH": "SSH",
    "8.Botnet": "Botnet",
    "9.Malware": "Malware",
    "10.SQL_Injection": "SQL_Injection",
    "11.SSDP": "SSDP",
    "12.Evil_Twin": "Evil_Twin",
    "13.Website_spoofing": "Website_spoofing",
}


def get_category_path(category: str) -> str:
    """Retorna la ruta de la carpeta de la categoría en Kaggle (ej. 'CSV/4.Rogue_AP')."""
    clean_cat = category.replace("CSV/", "").replace("CSV\\", "").strip().rstrip("/\\")
    return f"CSV/{clean_cat}"


def get_category_file_paths(category: str, limit_files: int = 1) -> List[str]:
    """
    Retorna la lista de rutas relativas de archivos CSV individuales para una categoría en Kaggle.
    Ejemplo: get_category_file_paths('4.Rogue_AP', limit_files=1) -> ['CSV/4.Rogue_AP/RogueAP_0.csv']
    """
    clean_cat = category.replace("CSV/", "").replace("CSV\\", "").strip().rstrip("/\\")
    prefix = CATEGORY_FILE_PREFIX.get(clean_cat)
    if not prefix:
        # Fallback de prefijo
        parts = clean_cat.split(".", 1)
        prefix = parts[1] if len(parts) > 1 else clean_cat

    count = max(1, limit_files)
    return [f"CSV/{clean_cat}/{prefix}_{i}.csv" for i in range(count)]


def normalize_kaggle_path(path_str: str) -> str:
    """
    Asegura que la ruta apunte a un archivo o ruta válida de AWID3 con prefijo 'CSV/'.
    Corrige variantes conocidas como 'Rogue_AP_0.csv' a 'RogueAP_0.csv'.
    """
    p = path_str.strip().replace("\\", "/")
    while "//" in p:
        p = p.replace("//", "/")
    if not p.startswith("CSV/"):
        p = f"CSV/{p}"

    # Corrección automática de inconsistencias conocidas en nombres de archivo de AWID3
    p = p.replace("4.Rogue_AP/Rogue_AP_", "4.Rogue_AP/RogueAP_")

    return p


def partition_file_list(files: List[str], ratio: float = 0.5) -> Tuple[List[str], List[str]]:
    """
    Divide una lista de archivos ordenados en dos subconjuntos (Batch y Streaming).
    Ejemplo: 4 archivos con ratio=0.5 -> Batch: [f0, f1], Streaming: [f2, f3].
    Si hay 1 solo archivo, retorna ([f0], [f0]) para que se procese por filas.
    """
    sorted_files = sorted(files)
    if len(sorted_files) <= 1:
        return sorted_files, sorted_files

    split_idx = max(1, int(len(sorted_files) * ratio))
    batch_files = sorted_files[:split_idx]
    stream_files = sorted_files[split_idx:]
    return batch_files, stream_files


def split_dataframe_rows(df: pd.DataFrame, ratio: float = 0.5) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Divide un DataFrame en dos mitades:
    - Batch: primera porción [0 : split_idx] (línea base histórica)
    - Streaming: segunda porción [split_idx :] (flujo en tiempo real)
    """
    total_rows = len(df)
    split_idx = int(total_rows * ratio)
    batch_df = df.iloc[:split_idx]
    stream_df = df.iloc[split_idx:]
    return batch_df, stream_df
