"""Demo antes/después: muestra cómo los índices de 002_indexes.sql cambian el
plan de ejecución sobre la tabla `alerts` de NexaSecure WIDS.

Captura EXPLAIN ANALYZE con los índices desactivados (BEFORE, forzando
seq scan), aplica 002_indexes.sql, y lo vuelve a capturar (AFTER) -- mismo
patrón que database/demo_indexes.py del repo de referencia del profesor,
adaptado a un solo motor (Postgres) porque NexaSecure no usa Mongo.

Requiere que `alerts` tenga datos (ver seed_alerts.py o dejar correr el job
de detección en streaming un rato) -- la diferencia seq scan vs index scan
solo es visible con volumen.

Uso (dentro del devcontainer de backend o con psycopg2 instalado):
    python3 -m database.rds.demo_indexes
"""
import datetime as dt
import os
import subprocess

import psycopg2

DB_DSN = os.environ['NEXASECURE_RDS_DSN']  # postgresql://user:pass@host:5432/nexasecure
MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), 'migrations')

QUERY = (
    'SELECT * FROM alerts '
    "WHERE rule_type = %s AND severity = %s "
    'ORDER BY detected_at DESC LIMIT 50'
)
QUERY_PARAMS = ('DEAUTH_FLOOD', 'HIGH')


def _run_sql_file(cursor, filename):
    path = os.path.join(MIGRATIONS_DIR, filename)
    with open(path, encoding='utf-8') as f:
        cursor.execute(f.read())


def _drop_optimization_indexes(cursor):
    cursor.execute('''
        DROP INDEX IF EXISTS idx_alerts_detected_at_brin;
        DROP INDEX IF EXISTS idx_alerts_details_gin;
        DROP INDEX IF EXISTS idx_incident_cases_open;
        DROP INDEX IF EXISTS idx_alerts_rule_severity;
    ''')


def _explain(cursor, query, params):
    cursor.execute(f'EXPLAIN (ANALYZE, FORMAT JSON) {query}', params)
    plan = cursor.fetchone()[0][0]
    return {
        'node_type': plan['Plan']['Node Type'],
        'rows_examined': plan['Plan'].get('Actual Rows', 0),
        'millis': plan['Execution Time'],
    }


def _print_stats(label, stats):
    print(f'  {label}')
    print(f'    Tipo de nodo:        {stats["node_type"]}')
    print(f'    Filas devueltas:     {stats["rows_examined"]}')
    print(f'    Tiempo de ejecución: {stats["millis"]:.2f} ms')


def _print_comparison(before, after):
    if after['millis'] > 0:
        speedup = f'{before["millis"] / after["millis"]:.1f}x más rápido'
    else:
        speedup = 'tan rápido que ya no se mide en ms'
    print(f'  >>> {speedup} ({before["node_type"]} -> {after["node_type"]}) <<<')


def main():
    connection = psycopg2.connect(DB_DSN)
    connection.autocommit = True
    with connection.cursor() as cursor:
        print('\n=== alerts: rule_type=DEAUTH_FLOOD AND severity=HIGH ===')

        _drop_optimization_indexes(cursor)
        before = _explain(cursor, QUERY, QUERY_PARAMS)
        _print_stats('ANTES (sin índices -- Seq Scan)', before)

        _run_sql_file(cursor, '002_indexes.sql')
        after = _explain(cursor, QUERY, QUERY_PARAMS)
        _print_stats('DESPUÉS (con índices -- Index Scan)', after)

        _print_comparison(before, after)
    connection.close()


if __name__ == '__main__':
    main()
