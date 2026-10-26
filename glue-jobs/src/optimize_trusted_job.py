"""One-off migration: preserve an unoptimized baseline, rewrite Trusted atomically.

Run while ingestion and maintenance are stopped. Do not expire snapshots until
the benchmark and validation are finished. Reruns recover from the baseline.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col

from iceberg_layout import LAYOUT_VERSION, configure_layout, table_exists, table_identifier, with_event_date


def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TRUSTED_BUCKET_NAME', 'CATALOG_DATABASE_NAME'])
    database = args['CATALOG_DATABASE_NAME']
    table = table_identifier(database, 'trusted_awid3')
    baseline = table_identifier(database, 'trusted_awid3_baseline')
    conf = SparkConf()
    conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    conf.set('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
    conf.set('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    conf.set('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    conf.set('spark.sql.catalog.glue_catalog.warehouse', f"s3://{args['TRUSTED_BUCKET_NAME']}/iceberg-warehouse/")
    conf.set('spark.sql.session.timeZone', 'UTC')
    context = GlueContext(SparkContext(conf=conf))
    spark = context.spark_session
    job = Job(context)
    job.init(args['JOB_NAME'], args)

    def current_snapshot():
        return spark.sql(f'SELECT snapshot_id FROM {table}.history '
                         'ORDER BY made_current_at DESC LIMIT 1').first()[0]

    starting_snapshot = current_snapshot()
    if not table_exists(spark, baseline):
        source = spark.read.option('snapshot-id', str(starting_snapshot)).table(table)
        enriched = with_event_date(source)
        if enriched.filter(col('event_date').isNull()).limit(1).count():
            raise RuntimeError('Invalid event epoch: refusing to create a misleading date partition.')
        (enriched.writeTo(baseline).using('iceberg')
         .tableProperty('format-version', '2')
         .tableProperty('location', f"s3://{args['TRUSTED_BUCKET_NAME']}/benchmark/trusted_awid3_baseline/")
         .tableProperty('nexasecure.source.snapshot', str(starting_snapshot)).create())
    reference = spark.table(baseline)
    reference_original = reference.drop('event_date')
    current_original = spark.read.option('snapshot-id', str(starting_snapshot)).table(table).drop('event_date')
    # Compare complete multisets, including duplicate rows, before any canonical writes.
    # Spark 3.3 column-pruning breaks count() on ExceptAll (missing attributes).
    # take(1) keeps the complete row projection and still stops at the first difference.
    if (reference_original.exceptAll(current_original).take(1)
            or current_original.exceptAll(reference_original).take(1)):
        raise RuntimeError('Trusted changed since baseline creation. Stop ingestion and investigate.')
    if current_snapshot() != starting_snapshot:
        raise RuntimeError('Concurrent writer detected. Stop ingestion and maintenance.')

    if 'event_date' not in spark.table(table).columns:
        spark.sql(f'ALTER TABLE {table} ADD COLUMN event_date DATE')
    # Inspect Iceberg metadata through its Java API to make retrying DDL safe.
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    fields = {field.name() for field in iceberg.spec().fields()}
    for field in ('event_date', 'Label'):
        if field not in fields:
            spark.sql(f'ALTER TABLE {table} ADD PARTITION FIELD `{field}`')
    configure_layout(spark, table, mark_complete=False)
    # Reads a distinct baseline table; overwrite commits a single new Iceberg snapshot.
    # Glue 4.0's Python WriterV2.overwrite fails to convert Column to Java.
    # SQL uses the same Iceberg atomic overwrite without the broken Python wrapper.
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'static')
    spark.sql(f'INSERT OVERWRITE TABLE {table} SELECT * FROM {baseline}')
    actual = spark.table(table)
    if (reference.exceptAll(actual).take(1)
            or actual.exceptAll(reference).take(1)):
        raise RuntimeError('Post-migration multiset mismatch: keep baseline and inspect snapshots.')
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('nexasecure.layout.version'='{LAYOUT_VERSION}')")
    print(f'VALIDATED: {actual.count()} rows; original snapshot={starting_snapshot}; '
          f'optimized snapshot={current_snapshot()}; baseline={baseline}')
    job.commit()


if __name__ == '__main__':
    main()
