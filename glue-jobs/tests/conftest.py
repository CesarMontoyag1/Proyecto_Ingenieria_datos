import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Creates a local Spark session for testing without requiring AWS credentials or Glue.
    """
    spark = SparkSession.builder \
        .appName("GlueJobsLocalTests") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()

