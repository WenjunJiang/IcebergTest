import os
from fastavro import writer, parse_schema

# ✅ Updated Avro schema with timestamp field
schema = {
    "type": "record",
    "name": "UserRecord",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "timestamp", "type": {
            "type": "long",
            "logicalType": "timestamp-millis"
        }},
        {"name": "user", "type": {
            "type": "record",
            "name": "UserInfo",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }},
        {"name": "location", "type": {
            "type": "record",
            "name": "LocationInfo",
            "fields": [
                {"name": "city", "type": "string"},
                {"name": "zip", "type": "string"}
            ]
        }}
    ]
}

parsed_schema = parse_schema(schema)

# ✅ Timestamps (milliseconds since epoch) for Jan–Apr 2025
timestamps = {
    "jan": 1735689600000,   # Jan 1, 2025
    "feb": 1738368000000,   # Feb 1, 2025
    "mar": 1740787200000,   # Mar 1, 2025
    "apr": 1743465600000    # Apr 1, 2025
}

data1 = [
    {
        "id": 1,
        "timestamp": timestamps["jan"],
        "user": {"name": "Alice", "age": 30},
        "location": {"city": "New York", "zip": "10001"},
    },
    {
        "id": 2,
        "timestamp": timestamps["feb"],
        "user": {"name": "Bob", "age": 25},
        "location": {"city": "Los Angeles", "zip": "90001"},
    },
]

data2 = [
    {
        "id": 3,
        "timestamp": timestamps["mar"],
        "user": {"name": "Charlie", "age": 35},
        "location": {"city": "Chicago", "zip": "60601"},
    },
    {
        "id": 4,
        "timestamp": timestamps["apr"],
        "user": {"name": "Diana", "age": 28},
        "location": {"city": "Houston", "zip": "77001"},
    },
]

def write_avro_file(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as out:
        writer(out, parsed_schema, records)

if __name__ == "__main__":
    write_avro_file("data2/source1/user_data1.avro", data1)
    write_avro_file("data2/source2/user_data2.avro", data2)
