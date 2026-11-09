"""The SPICE refresh Lambda reloads the batch datasets after a successful Glue ingestion."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'infrastructure' / 'serverless' / 'quicksight'))
from spice_refresh import dataset_ids, lambda_handler  # noqa: E402

EVENT = {"source": "aws.glue", "detail-type": "Glue Job State Change",
         "detail": {"jobName": "nexasecure_batch_ingest_dev_local", "state": "SUCCEEDED", "jobRunId": "jr_1"}}
ENV = {"QUICKSIGHT_ACCOUNT_ID": "123456789012", "DATASET_IDS": "nexasecure-traffic-dev, nexasecure-topbssid-dev"}


class AwsError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def test_dataset_ids_ignores_blanks():
    assert dataset_ids(" a, ,b ") == ["a", "b"]


def test_starts_one_full_refresh_per_dataset():
    client = MagicMock()
    with patch.dict("os.environ", ENV):
        result = lambda_handler(EVENT, None, client=client)
    assert result == {"started": ["nexasecure-traffic-dev", "nexasecure-topbssid-dev"], "skipped": [], "failed": []}
    calls = client.create_ingestion.call_args_list
    assert [c.kwargs["DataSetId"] for c in calls] == ["nexasecure-traffic-dev", "nexasecure-topbssid-dev"]
    assert all(c.kwargs["AwsAccountId"] == "123456789012" for c in calls)
    assert all(c.kwargs["IngestionType"] == "FULL_REFRESH" for c in calls)
    assert len({c.kwargs["IngestionId"] for c in calls}) == 2


def test_refresh_already_running_is_not_an_error():
    client = MagicMock()
    client.create_ingestion.side_effect = [AwsError("LimitExceededException"), None]
    with patch.dict("os.environ", ENV):
        result = lambda_handler(EVENT, None, client=client)
    assert result["skipped"] == ["nexasecure-traffic-dev"]
    assert result["started"] == ["nexasecure-topbssid-dev"]


def test_real_failure_raises_so_lambda_reports_an_error():
    client = MagicMock()
    client.create_ingestion.side_effect = AwsError("AccessDeniedException")
    with patch.dict("os.environ", ENV), pytest.raises(RuntimeError, match="AccessDeniedException|failed"):
        lambda_handler(EVENT, None, client=client)


def test_account_id_falls_back_to_function_arn():
    client = MagicMock()
    context = MagicMock(invoked_function_arn="arn:aws:lambda:us-east-1:999988887777:function:x")
    with patch.dict("os.environ", {"DATASET_IDS": "nexasecure-traffic-dev"}, clear=True):
        lambda_handler(EVENT, context, client=client)
    assert client.create_ingestion.call_args.kwargs["AwsAccountId"] == "999988887777"
