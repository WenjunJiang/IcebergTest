from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def flatten_df(df, required_columns):
    # The current df columns order corresponds to required_columns, but with short names
    new_names = ["__".join(col_path.split(".")) for col_path in required_columns]
    return df.toDF(*new_names)

# Spark session
spark = SparkSession.builder \
    .appName("Efficient Query Iceberg") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "warehouse") \
    .getOrCreate()

table = "local.db.users_nest"
target_ids = [1, 3]

required_columns = [
    "id",
    "user.age",
    "location.city"
]

df = spark.read.format("iceberg").load(table).select(*required_columns)

df_filtered = df.filter(col("id").isin(target_ids))

# Flatten with full names
df_flat = flatten_df(df_filtered, required_columns)

df_flat.show(truncate=False)