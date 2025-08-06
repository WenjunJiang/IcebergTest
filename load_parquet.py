from pyspark.sql import SparkSession

def load_parquet_to_iceberg(parquet_path: str, warehouse_path: str, table: str):
    spark = (
        SparkSession.builder
        .appName("LoadParquetToIceberg")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .getOrCreate()
    )

    df = spark.read.parquet(parquet_path)
    df.writeTo(table).using("iceberg").createOrReplace()
    print(f"✅ Loaded {parquet_path} into Iceberg table {table}")
    spark.stop()

if __name__ == "__main__":
    load_parquet_to_iceberg(
        parquet_path="./data/sample.parquet",
        warehouse_path="./warehouse",
        table="local.db.users"
    )
