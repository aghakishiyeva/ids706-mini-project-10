from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, col
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("PySpark Data Processing") \
        .getOrCreate()

def read_dataset(spark, file_path: str):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def generate_summary_statistics(df):
    summary_stats = df.select([mean(c).alias(c) for c in df.columns])
    return summary_stats

def save_summary_to_markdown(df, file_path: str) -> None:
    with open(file_path, 'w') as f:
        for column in df.columns:
            f.write(f"## {column.capitalize()}\n")
            row = df.select(column).collect()[0]
            for metric, value in row.asDict().items():
                f.write(f"- {metric}: {value}\n")
            f.write("\n")

def main():
    spark = create_spark_session()
    data = read_dataset(spark, 'src/winequality-red.csv')

    summary = generate_summary_statistics(data)
    summary.show()

    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    save_summary_to_markdown(summary, os.path.join(output_dir, 'summary.md'))

if __name__ == "__main__":
    main()
