"""Create or replace the Serving views in Athena (consumed by QuickSight).

Usage (from the repo root, with .envs/aws.env):
    python scripts/athena_views.py --stage dev            # create/replace the views
    python scripts/athena_views.py --stage dev --dry-run  # print the SQL only
    python scripts/athena_views.py --stage dev --check    # also run a sample query per view
"""
import argparse
import re
import time

from aws_common import ROOT, aws_session, parameter
from iceberg_benchmark import run_query


def execute(client, sql, database, workgroup, timeout=300):
    """Run a statement that returns no rows (DDL) and wait until it finishes."""
    query_id = client.start_query_execution(
        QueryString=sql, WorkGroup=workgroup,
        QueryExecutionContext={'Database': database, 'Catalog': 'AwsDataCatalog'},
    )['QueryExecutionId']
    deadline = time.monotonic() + timeout
    while True:
        status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']
        if status['State'] == 'SUCCEEDED':
            return query_id
        if status['State'] in ('FAILED', 'CANCELLED'):
            raise RuntimeError(f'Athena {query_id}: {status.get("StateChangeReason")}')
        if time.monotonic() > deadline:
            client.stop_query_execution(QueryExecutionId=query_id)
            raise TimeoutError(f'Athena query timed out: {query_id}')
        time.sleep(1)

SQL_FILE = ROOT / 'infrastructure' / 'serverless' / 'consumption' / 'sql' / 'serving_views.sql'
_IDENTIFIER = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*')
_VIEW_NAME = re.compile(r'CREATE\s+OR\s+REPLACE\s+VIEW\s+"[^"]+"\."([^"]+)"', re.IGNORECASE)


def load_statements(sql_text, database):
    """Split the file into statements (";" at end of line) and fill in the database."""
    if not _IDENTIFIER.fullmatch(database):
        raise ValueError('Invalid database identifier')
    lines = [line for line in sql_text.splitlines() if not line.strip().startswith('--')]
    statements, current = [], []
    for line in lines:
        current.append(line)
        if line.rstrip().endswith(';'):
            statement = '\n'.join(current).strip().rstrip(';').strip()
            if statement:
                statements.append(statement.replace('{database}', database))
            current = []
    if '\n'.join(current).strip():
        raise ValueError('The last statement does not end with ";"')
    return statements


def view_name(statement):
    match = _VIEW_NAME.search(statement)
    if not match:
        raise ValueError('Only CREATE OR REPLACE VIEW statements are allowed')
    return match.group(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--stage', default='dev')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--check', action='store_true', help='run SELECT ... LIMIT 5 on each view')
    args = parser.parse_args()

    session = aws_session()
    database = parameter(session, args.stage, 'glue/GlueCatalogDatabaseName')
    workgroup = parameter(session, args.stage, 'consumption/AthenaWorkGroup')
    trusted = parameter(session, args.stage, 'storage/TrustedAwid3BucketName')
    output = f's3://{trusted}/athena-results/consumption/'
    statements = load_statements(SQL_FILE.read_text(encoding='utf-8'), database)

    client = session.client('athena')
    for statement in statements:
        name = view_name(statement)
        if args.dry_run:
            print(f'-- {name}\n{statement};\n')
            continue
        query_id = execute(client, statement, database, workgroup)
        print(f'OK  {database}.{name}  (query {query_id})')
        if args.check:
            sample = run_query(client, f'SELECT * FROM "{database}"."{name}" LIMIT 5',
                               database, output, workgroup)
            print(f'    {len(sample["rows"])} filas de muestra, columnas: {", ".join(sample["columns"])}')


if __name__ == '__main__':
    main()
