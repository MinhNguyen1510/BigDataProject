
FROM python:3.8-slim-bullseye

USER root

# 1. Cài đặt Java (Spark cần Java) và các công cụ cần thiết
RUN apt-get update && apt-get install -y openjdk-11-jre-headless curl procps && \
       rm -rf /var/lib/apt/lists/*
# 2. Cấu hình biến môi trường Spark
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Khai báo rõ ràng phiên bản Python cho Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 3. Tải và cài đặt Spark 3.3.2
#RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz && \
#       tar -xf spark.tgz && \
#       mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
#       rm spark.tgz
COPY spark-3.3.2-bin-hadoop3.tgz /tmp/spark.tgz
RUN tar -xf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.3.2-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# 4. Tải các thư viện JAR (S3, Delta, MySQL)

WORKDIR $SPARK_HOME/jars
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar

WORKDIR $SPARK_HOME