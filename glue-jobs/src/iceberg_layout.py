"""Trusted layout shared by ingestion and the one-off migration (Glue 4.0)."""
import re

LAYOUT_VERSION = '3'
LAYOUT_PROPERTIES = {
    'write.parquet.bloom-filter-enabled.column.wlan.bssid': 'true',
    # Iceberg 1.0 sanitizes dots in physical Parquet names (verified in file footers).
    'write.parquet.bloom-filter-enabled.column.wlan_x2Ebssid': 'true',
    # BSSIDs have low cardinality; 1 MiB per group costs more I/O than it saves.
    'write.parquet.bloom-filter-max-bytes': '4096',
    'write.metadata.metrics.column.wlan.bssid': 'full',
    'write.distribution-mode': 'hash',
    'nexasecure.layout.version': LAYOUT_VERSION,
}


def table_identifier(database, table):
    for name in (database, table):
        if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', name):
            raise ValueError('Invalid database/table identifier')
    return f'glue_catalog.{database}.{table}'


def table_exists(spark, table):
    # Spark 3.3 Catalog.tableExists does not reliably resolve a three-part V2 name.
    namespace, name = table.rsplit('.', 1)
    return bool(spark.sql(f"SHOW TABLES IN {namespace} LIKE '{name}'").take(1))


def with_event_date(df):
    from pyspark.sql.functions import col, to_date
    # Spark session timezone MUST be UTC (configured by both callers).
    return df.withColumn('event_date', to_date(col('`frame.time_epoch`').cast('double').cast('timestamp')))


def configure_layout(spark, table, mark_complete=True):
    properties = ', '.join(f"'{key}'='{value}'" for key, value in LAYOUT_PROPERTIES.items()
                           if mark_complete or key != 'nexasecure.layout.version')
    spark.sql(f'ALTER TABLE {table} SET TBLPROPERTIES ({properties})')
    spark.sql(f'ALTER TABLE {table} WRITE ORDERED BY `wlan.bssid` ASC NULLS LAST')


def assert_migrated(spark, table):
    if table_exists(spark, table):
        properties = dict((row[0], row[1]) for row in spark.sql(f'SHOW TBLPROPERTIES {table}').collect())
        if properties.get('nexasecure.layout.version') != LAYOUT_VERSION:
            raise RuntimeError('Trusted exists without the new layout. Run TrustedOptimizationJob '
                               'before ingestion; do not reset bookmarks or reload Raw.')


def save_trusted(df, table, location, spark):
    assert_migrated(spark, table)
    enriched = with_event_date(df)
    if not table_exists(spark, table):
        (enriched.limit(0).writeTo(table).using('iceberg')
         .tableProperty('format-version', '2').tableProperty('location', location)
         .partitionedBy('event_date', 'Label').create())
        configure_layout(spark, table)
    enriched.writeTo(table).append()
