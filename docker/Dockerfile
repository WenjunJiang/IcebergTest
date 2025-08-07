FROM openjdk:11-jdk-slim

ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3
ENV SCALA_VERSION=2.12
ENV ICEBERG_VERSION=1.9.2

RUN apt-get update && apt-get install -y wget python3 python3-pip && \
    pip3 install pyspark==${SPARK_VERSION}

RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Add Iceberg JAR
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar -P $SPARK_HOME/jars

# Add Spark Avro JAR
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar -P $SPARK_HOME/jars

WORKDIR /app