Go to the folder "C:\Users\jiang\PycharmProjects\icebergTest\docker"
```shell
docker build --no-cache -t spark-iceberg .
docker run -it --name spark-iceberg-container spark-iceberg
```
Go to the folder "C:\Users\jiang\PycharmProjects\icebergTest"
```shell
pip install pandas pyspark==3.5.6 pyarrow fastavro
python prepare_sample_parquet.py
## In windows
docker run --rm -v "%cd%:/app" -w /app spark-iceberg spark-submit load_parquet.py
docker run --rm -v "%cd%:/app" -w /app spark-iceberg spark-submit query_iceberg.py
docker run --rm -v "%cd%:/app" -w /app spark-iceberg spark-submit load_avro.py
# or in mac and Linux
docker run --rm -v "$(pwd):/app" -w /app spark-iceberg spark-submit load_parquet.py
docker run --rm -v "$(pwd):/app" -w /app spark-iceberg spark-submit query_iceberg.py
```
To test multiple avro files with nested columns
```shell
python create_nested_avro_files.py
docker run --rm -v "%cd%:/app" -v "%cd%/data2:data" -w /app spark-iceberg spark-submit load_avro_recursive.py
docker run --rm -v "%cd%:/app" -v "%cd%/data2:data" -w /app spark-iceberg spark-submit query_iceberg_and_flatten.py
# or in mac and Linux
docker run --rm -v "$(pwd):/app" -v "$(pwd)/data2:data" -w /app spark-iceberg spark-submit load_avro_recursive.py
docker run --rm -v "$(pwd):/app" -v "$(pwd)/data2:data" -w /app spark-iceberg spark-submit query_iceberg_and_flatten.py
```

