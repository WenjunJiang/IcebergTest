import os
from fastavro import writer, parse_schema

# Define the Avro schema matching the Parquet example
schema = {
    "doc": "User record",
    "name": "User",
    "namespace": "example.avro",
    "type": "record",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
    ]
}

# Data to write (same as your Parquet example)
records = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 35}
]

# Ensure output directory exists
os.makedirs("data", exist_ok=True)

# Write to Avro
with open("data/sample.avro", "wb") as out:
    writer(out, parse_schema(schema), records)