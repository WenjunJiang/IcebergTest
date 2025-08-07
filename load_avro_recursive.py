# load_avro_to_iceberg.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import os

def load_avro_to_iceberg(input_dir: str, warehouse_path: str, table: str):
    spark = (
        SparkSession.builder
        .appName("Load Avro to Iceberg")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .getOrCreate()
    )

    # Recursively read all Avro files
    df = spark.read.format("avro").load(f"{input_dir}/**/*.avro", recursiveFileLookup=True)

    # Print schema to debug mismatches
    df.printSchema()

    df.writeTo(table).using("iceberg").createOrReplace()

    spark.stop()

if __name__ == "__main__":
    load_avro_to_iceberg(
        input_dir="./data2",
        warehouse_path="./warehouse",
        table="local.db.users_nest"
    )
