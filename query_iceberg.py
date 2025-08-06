from pyspark.sql import SparkSession

def query_table(warehouse_path: str, table: str):
    spark = (
        SparkSession.builder
        .appName("QueryIcebergTable")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .getOrCreate()
    )

    df = spark.read.table(table)
    df.select("name", "age").where("age > 30").show()
    spark.stop()

if __name__ == "__main__":
    query_table(
        warehouse_path="./warehouse",
        table="local.db.users"
    )
