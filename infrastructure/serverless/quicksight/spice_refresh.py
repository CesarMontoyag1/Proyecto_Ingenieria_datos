"""Recarga en SPICE los datasets de tráfico cuando la ingesta batch de Glue termina bien.

La invoca una regla de EventBridge ("Glue Job State Change", estado SUCCEEDED del job de
ingesta). Así el dashboard muestra los datos batch nuevos sin refrescos manuales.
"""
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Errores que indican que ya hay una carga en curso: no es un fallo, la carga vigente traerá los datos.
_ALREADY_RUNNING = ("LimitExceededException", "ResourceExistsException", "ConflictException")


def dataset_ids(raw: Optional[str] = None) -> List[str]:
    value = raw if raw is not None else os.environ.get("DATASET_IDS", "")
    return [ds.strip() for ds in value.split(",") if ds.strip()]


def account_id(context: Any) -> str:
    if os.environ.get("QUICKSIGHT_ACCOUNT_ID"):
        return os.environ["QUICKSIGHT_ACCOUNT_ID"]
    # arn:aws:lambda:<region>:<account>:function:<name>
    return context.invoked_function_arn.split(":")[4]


def lambda_handler(event: Dict[str, Any], context: Any, client=None) -> Dict[str, Any]:
    detail = event.get("detail", {})
    logger.info("Evento de Glue: job=%s estado=%s run=%s",
                detail.get("jobName"), detail.get("state"), detail.get("jobRunId"))

    quicksight = client or boto3.client("quicksight")
    account = account_id(context)
    started, skipped, failed = [], [], []

    for ds in dataset_ids():
        ingestion_id = f"auto-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        try:
            quicksight.create_ingestion(AwsAccountId=account, DataSetId=ds,
                                        IngestionId=ingestion_id, IngestionType="FULL_REFRESH")
            started.append(ds)
            logger.info("Recarga SPICE iniciada: %s (%s)", ds, ingestion_id)
        except Exception as e:  # noqa: BLE001 - se clasifica por nombre del error de AWS
            code = getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
            if code in _ALREADY_RUNNING:
                skipped.append(ds)
                logger.info("Ya hay una recarga en curso para %s (%s)", ds, code)
            else:
                failed.append(ds)
                logger.error("No se pudo recargar %s: %s", ds, e)

    result = {"started": started, "skipped": skipped, "failed": failed}
    if failed:
        # Hace visible el error en la métrica Errors de Lambda (y Lambda reintenta el evento).
        raise RuntimeError(f"Recarga SPICE fallida: {result}")
    return result
