"""The Serving views must be valid, governed and bound to the right database."""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from athena_views import SQL_FILE, execute, load_statements, view_name  # noqa: E402


def statements():
    return load_statements(SQL_FILE.read_text(encoding='utf-8'), 'nexasecure_wids_catalog_dev_local')


def test_every_statement_is_a_view_in_the_target_database():
    names = [view_name(s) for s in statements()]
    assert names == ['vw_alerts', 'vw_alerts_hourly', 'vw_traffic_daily', 'vw_top_bssid']
    for statement in statements():
        assert '{database}' not in statement
        assert '"nexasecure_wids_catalog_dev_local"."vw_' in statement


def test_views_never_read_raw_or_quarantine():
    for statement in statements():
        lowered = statement.lower()
        assert 'raw_awid3' not in lowered
        assert 'quarantine' not in lowered


def test_top_bssid_enforces_minimum_group_size():
    top = next(s for s in statements() if view_name(s) == 'vw_top_bssid')
    assert 'HAVING COUNT(*) >= 5' in top


def test_invalid_database_is_rejected():
    with pytest.raises(ValueError):
        load_statements('SELECT 1;', 'db"; DROP TABLE x; --')


def test_unterminated_statement_is_rejected():
    with pytest.raises(ValueError):
        load_statements('CREATE OR REPLACE VIEW "{database}"."v" AS SELECT 1', 'db')


def test_ddl_execution_uses_the_consumption_workgroup_and_reports_failures():
    client = MagicMock()
    client.start_query_execution.return_value = {'QueryExecutionId': 'q-1'}
    client.get_query_execution.return_value = {'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}}
    assert execute(client, 'CREATE OR REPLACE VIEW ...', 'db', 'nexasecure-consumption-dev') == 'q-1'
    assert client.start_query_execution.call_args.kwargs['WorkGroup'] == 'nexasecure-consumption-dev'

    client.get_query_execution.return_value = {'QueryExecution': {
        'Status': {'State': 'FAILED', 'StateChangeReason': 'COLUMN_NOT_FOUND'}}}
    with pytest.raises(RuntimeError, match='COLUMN_NOT_FOUND'):
        execute(client, 'CREATE OR REPLACE VIEW ...', 'db', 'wg')
