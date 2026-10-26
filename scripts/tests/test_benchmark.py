"""Benchmark evidence must come from real execution and preserve literal filters."""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from iceberg_benchmark import literal, query_sql, run_query


def test_quotes_in_label_cannot_change_the_predicate():
    sql = query_sql('catalog_dev', {'date': '2020-12-17',
                    'label': "attack' OR true --", 'bssid': '00:11:22:12345678'}, False)
    assert '"Label" = \'attack\'\' OR true --\'' in sql['date_label']
    assert literal("a'b") == "'a''b'"
    with pytest.raises(ValueError):
        query_sql('db; DROP TABLE alerts', {}, False)


def test_benchmark_requests_real_execution_and_keeps_null_results():
    client = MagicMock()
    client.start_query_execution.return_value = {'QueryExecutionId': 'query-1'}
    client.get_query_execution.return_value = {'QueryExecution': {
        'Status': {'State': 'SUCCEEDED'},
        'Statistics': {'EngineExecutionTimeInMillis': 25, 'DataScannedInBytes': 120},
    }}
    client.get_paginator.return_value.paginate.return_value = [{'ResultSet': {'Rows': [
        {'Data': [{'VarCharValue': 'count'}, {'VarCharValue': 'sum'}]},
        {'Data': [{'VarCharValue': '0'}, {}]},
    ]}}]
    result = run_query(client, 'SELECT 1', 'db', 's3://bucket/results/', 'primary')
    assert result['rows'] == [['0', None]]
    assert result['bytes_scanned'] == 120
    assert client.start_query_execution.call_args.kwargs['ResultReuseConfiguration'] == {
        'ResultReuseByAgeConfiguration': {'Enabled': False}}


def test_failed_query_cannot_produce_successful_evidence():
    client = MagicMock()
    client.start_query_execution.return_value = {'QueryExecutionId': 'failed-1'}
    client.get_query_execution.return_value = {'QueryExecution': {
        'Status': {'State': 'FAILED', 'StateChangeReason': 'access denied'}}}
    with pytest.raises(RuntimeError, match='failed-1.*access denied'):
        run_query(client, 'SELECT 1', 'db', 's3://bucket/results/', 'primary')
    client.get_paginator.assert_not_called()
