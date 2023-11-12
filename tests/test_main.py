from pyspark.sql import SparkSession
from main import read_dataset

def test_read_dataset():
    spark = SparkSession.builder \
        .appName("PySpark Tests") \
        .getOrCreate()

    df = read_dataset(spark, 'tests/test.csv')
    assert df.count() > 0
