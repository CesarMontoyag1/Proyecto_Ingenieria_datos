import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def main():
    raw_args = sys.argv
    opt_keys = ['JOB_NAME', 'TRUSTED_BUCKET_NAME']
    if any('--CATALOG_DATABASE_NAME' in arg for arg in raw_args):
        opt_keys.append('CATALOG_DATABASE_NAME')

    args = getResolvedOptions(raw_args, opt_keys)
    trusted_bucket = args['TRUSTED_BUCKET_NAME']
    catalog_db = args.get('CATALOG_DATABASE_NAME', 'nexasecure_wids_catalog_dev_local')

    conf = SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{trusted_bucket}/iceberg-warehouse/")

    sc = SparkContext(conf=conf)
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args['JOB_NAME'], args)
        
    print(f"Spark Session initialized. Starting Iceberg maintenance on database {catalog_db}...")
    
    # 1. Rewrite Data Files (Compaction)
    print("Compacting small files in Trusted table...")
    spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{catalog_db}.trusted_awid3')")
    
    # 2. Expire Snapshots (Maintenance)
    print("Expiring old snapshots to free space...")
    spark.sql(f"CALL glue_catalog.system.expire_snapshots(table => '{catalog_db}.trusted_awid3', retain_last => 5)")
    
    print("Maintenance Job completed successfully.")
    job.commit()

if __name__ == "__main__":
    main()
