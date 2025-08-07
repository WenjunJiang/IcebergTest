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
    df = spark.read.format("avro").load(f"{input_dir}", recursiveFileLookup=True)

    # Print schema to debug mismatches
    df.printSchema()

    df.writeTo(table).using("iceberg").createOrReplace()

    spark.stop()

if __name__ == "__main__":
    # The script is mapped to /app in the container in your cmd: -v "XXX:/app"

    # Your Avro data is now mounted at /data inside the container.
    input_directory_inside_container = "/data" # mounted to your local data folder in cmd: -v "XXX:/data"

    # The warehouse directory is still managed within /app for simplicity,
    # assuming you want your Iceberg tables to be alongside your script.
    warehouse_directory_inside_container = "/app/warehouse"

    table_name = "local.db.users_nest"

    load_avro_to_iceberg(
        input_dir=input_directory_inside_container,
        warehouse_path=warehouse_directory_inside_container,
        table=table_name
    )
