from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, col

def create_spark_session():
    return SparkSession.builder \
        .appName("PySpark Data Processing") \
        .getOrCreate()

def read_dataset(spark, file_path: str):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def generate_summary_statistics(df):
    summary = df.select([mean(c).alias(c) for c in df.columns])
    return summary

def main():
    spark = create_spark_session()
    data = read_dataset(spark, 'src/winequality-red.csv')

    summary = generate_summary_statistics(data)
    summary.show()

if __name__ == "__main__":
    main()
