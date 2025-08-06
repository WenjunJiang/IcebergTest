from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    print("CWD:", os.getcwd())

    spark = (
        SparkSession.builder
        .appName("Debug Spark")
        .master("local[*]")  # Run locally using all cores
        .getOrCreate()
    )

    df = spark.read.parquet("./data/sample.parquet")
    df.show()

    spark.stop()
