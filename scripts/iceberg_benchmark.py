"""Measure Athena before/after on the same Trusted rows, with result reuse off."""
import argparse
import datetime as dt
import json
import re
import statistics
import time
from pathlib import Path

from aws_common import ROOT, aws_session, parameter


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def query_sql(database, selection, optimized):
    if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', database):
        raise ValueError('Invalid database identifier')
    date = 'event_date' if optimized else 'CAST(from_unixtime(TRY_CAST("frame.time_epoch" AS double)) AS date)'
    aggregate = 'SELECT COUNT(*) AS records, SUM(TRY_CAST("frame.len" AS double)) AS frame_bytes '
    source = f'FROM "{database}"."trusted_awid3" '
    return {
        'date_label': aggregate + source + f'WHERE {date} = DATE {literal(selection["date"])} '
                      + f'AND "Label" = {literal(selection["label"])}',
        'bssid': aggregate + source + f'WHERE "wlan.bssid" = {literal(selection["bssid"])}',
    }


def run_query(client, sql, database, output, workgroup):
    started = client.start_query_execution(
        QueryString=sql, QueryExecutionContext={'Database': database, 'Catalog': 'AwsDataCatalog'},
        WorkGroup=workgroup, ResultConfiguration={'OutputLocation': output},
        ResultReuseConfiguration={'ResultReuseByAgeConfiguration': {'Enabled': False}},
    )
    query_id = started['QueryExecutionId']
    deadline = time.monotonic() + 300
    while True:
        execution = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']
        status = execution['Status']['State']
        if status in ('FAILED', 'CANCELLED'):
            raise RuntimeError(f'Athena {query_id}: {execution["Status"].get("StateChangeReason")}')
        if status == 'SUCCEEDED':
            break
        if time.monotonic() > deadline:
            client.stop_query_execution(QueryExecutionId=query_id)
            raise TimeoutError(f'Athena query timed out: {query_id}')
        time.sleep(1)
    pages = client.get_paginator('get_query_results').paginate(QueryExecutionId=query_id)
    rows = [row for page in pages for row in page['ResultSet']['Rows']]
    values = [[cell.get('VarCharValue') for cell in row['Data']] for row in rows]
    stats = execution['Statistics']
    return {
        'query_id': query_id, 'sql': sql, 'columns': values[0], 'rows': values[1:],
        'engine_ms': stats.get('EngineExecutionTimeInMillis', 0),
        'total_ms': stats.get('TotalExecutionTimeInMillis', 0),
        'bytes_scanned': stats.get('DataScannedInBytes', 0),
        'reused': stats.get('ResultReuseInformation', {}).get('ReusedPreviousResult', False),
    }


def metadata(session, database, table_name='trusted_awid3'):
    table = session.client('glue').get_table(DatabaseName=database, Name=table_name)['Table']
    location = table['Parameters']['metadata_location']
    bucket, key = location[5:].split('/', 1)
    data = json.loads(session.client('s3').get_object(Bucket=bucket, Key=key)['Body'].read())
    snapshot = next(x for x in data['snapshots'] if x['snapshot-id'] == data['current-snapshot-id'])
    return {
        'metadata_location': location, 'snapshot_id': snapshot['snapshot-id'],
        'manifest_list': snapshot['manifest-list'],
        'total_records': snapshot['summary']['total-records'],
        'partition_specs': data.get('partition-specs'), 'sort_orders': data.get('sort-orders'),
        'properties': data.get('properties', {}),
    }


def report(directory, before, after):
    lines = ['# Athena: optimización de Trusted', '',
             'Mismas filas y filtros; reutilización de resultados desactivada. '
             'Medianas de ejecuciones; los cambios se miden como conjunto, '
             'sin atribuir por separado beneficios a bloom filters.', '',
             '| Consulta | Antes ms | Después ms | Antes bytes | Después bytes | Reducción bytes |',
             '|---|---:|---:|---:|---:|---:|']
    for name in before['queries']:
        b, a = before['queries'][name], after['queries'][name]
        before_bytes = statistics.median(x['bytes_scanned'] for x in b)
        after_bytes = statistics.median(x['bytes_scanned'] for x in a)
        reduction = f'{100 * (1 - after_bytes / before_bytes):.2f}%' if before_bytes else 'N/A'
        lines.append(f'| {name} | {statistics.median(x["engine_ms"] for x in b)} '
                     f'| {statistics.median(x["engine_ms"] for x in a)} '
                     f'| {before_bytes} | {after_bytes} | {reduction} |')
    lines += ['', 'Los JSON contienen SQL, identificadores de consulta, snapshots y resultados '
              'para reproducir/verificar el comparativo.', '',
              'Una mejora de tiempo no está garantizada en un dataset pequeño. '
              'La equivalencia de resultados es obligatoria.']
    (directory / 'resultados.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['before', 'after'])
    parser.add_argument('--stage', default='dev')
    parser.add_argument('--workgroup', default='primary')
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--directory', type=Path, default=ROOT / 'docs/optimizacion/iceberg/evidencia')
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error('At least one repetition is required')
    directory = args.directory
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f'{args.phase}.json'
    if destination.exists():
        raise RuntimeError('Evidence exists. Use a new --directory for another experiment.')
    session = aws_session()
    database = parameter(session, args.stage, 'glue/GlueCatalogDatabaseName')
    trusted = parameter(session, args.stage, 'storage/TrustedAwid3BucketName')
    output = f's3://{trusted}/athena-results/optimization/'
    client = session.client('athena')
    initial = metadata(session, database)
    before = None
    if args.phase == 'before':
        if initial['properties'].get('nexasecure.layout.version'):
            raise RuntimeError('Trusted is already optimized; cannot capture a before measurement.')
        sample = run_query(client,
            'SELECT CAST(from_unixtime(TRY_CAST("frame.time_epoch" AS double)) AS date) AS day, '
            '"Label", COUNT(*) AS n FROM trusted_awid3 GROUP BY 1, 2 ORDER BY n DESC, 1, 2 LIMIT 1',
            database, output, args.workgroup)['rows'][0]
        bssid = run_query(client,
            'SELECT "wlan.bssid", COUNT(*) AS n FROM trusted_awid3 '
            'WHERE "wlan.bssid" IS NOT NULL AND trim("wlan.bssid") <> \'\' '
            'AND substr(lower("wlan.bssid"), 1, 8) <> \'ff:ff:ff\' '
            'GROUP BY 1 ORDER BY n DESC, 1 LIMIT 1',
            database, output, args.workgroup)['rows'][0][0]
        selection = {'date': sample[0], 'label': sample[1], 'bssid': bssid}
    else:
        before = json.loads((directory / 'before.json').read_text(encoding='utf-8'))
        if before['database'] != database or before['stage'] != args.stage:
            raise RuntimeError('Before evidence belongs to another environment.')
        if before['metadata']['total_records'] != initial['total_records']:
            raise RuntimeError('Row counts changed: the benchmark must use the same dataset.')
        if initial['properties'].get('nexasecure.layout.version') != '3':
            raise RuntimeError('Trusted migration has not completed.')
        baseline = metadata(session, database, 'trusted_awid3_baseline')
        if baseline['properties'].get('nexasecure.source.snapshot') != str(before['metadata']['snapshot_id']):
            raise RuntimeError('Optimized data does not reference the before snapshot.')
        selection = before['selection']
    queries = {}
    for name, sql in query_sql(database, selection, args.phase == 'after').items():
        queries[name] = []
        for _ in range(args.repetitions):
            result = run_query(client, sql, database, output, args.workgroup)
            if result['reused']:
                raise RuntimeError('Unexpected result reuse')
            expected = before['queries'][name][0]['rows'] if before else queries[name][0]['rows'] if queries[name] else result['rows']
            if result['rows'] != expected:
                raise RuntimeError(f'Result mismatch: {name}')
            queries[name].append(result)
            print(f'{args.phase} {name}: {result["engine_ms"]} ms, {result["bytes_scanned"]} bytes')
    if metadata(session, database)['snapshot_id'] != initial['snapshot_id']:
        raise RuntimeError('Concurrent writes during benchmark. Evidence not saved.')
    data = {'phase': args.phase, 'stage': args.stage, 'database': database,
            'captured_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
            'selection': selection, 'metadata': initial, 'queries': queries}
    destination.write_text(json.dumps(data, indent=2) + '\n', encoding='utf-8')
    if before:
        report(directory, before, data)
    print(f'Evidence saved: {destination}')


if __name__ == '__main__':
    main()
