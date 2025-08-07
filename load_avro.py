# load_avro.py
from pyspark.sql import SparkSession

def load_avro_to_iceberg(avro_path: str, warehouse_path: str, table: str):
    spark = (
        SparkSession.builder
        .appName("LoadAvroToIceberg")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .getOrCreate()
    )

    df = spark.read.format("avro").load(avro_path)

    df.writeTo(table).using("iceberg").createOrReplace()

    print(f"✅ Loaded Avro data from {avro_path} to Iceberg table {table}")
    spark.stop()

if __name__ == "__main__":
    load_avro_to_iceberg(
        avro_path="./data/sample.avro",
        warehouse_path="./warehouse",
        table="local.db.users"
    )