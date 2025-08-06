Go to the folder "C:\Users\jiang\PycharmProjects\icebergTest\docker"
```shell
docker build -t spark-iceberg .
docker run -it --name spark-iceberg-container spark-iceberg
```
Go to the folder "C:\Users\jiang\PycharmProjects\icebergTest"
```shell
pip install pandas pyspark==3.5.6 pyarrow
python prepare_sample_parquet.py
docker run --rm -v "%cd%:/app" -w /app spark-iceberg spark-submit load_parquet.py
docker run --rm -v "%cd%:/app" -w /app spark-iceberg spark-submit query_iceberg.py
```

