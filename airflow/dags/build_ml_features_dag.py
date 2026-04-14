from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "start_date": datetime(2026, 4, 12),
}

SPARK_COMMON_CONF = {
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minio",
    "spark.hadoop.fs.s3a.secret.key": "minio123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
    "spark.hadoop.fs.s3a.multipart.size": "10485760",
    "spark.hadoop.fs.s3a.threads.max": "20",
    "spark.sql.autoBroadcastJoinThreshold": "10485760",
    "spark.sql.join.preferSortMergeJoin": "true",
    "spark.executor.memoryOverhead": "256m",
    "spark.driver.memoryOverhead": "256m",
    "spark.cores.max": "2",
    "spark.sql.shuffle.partitions": "4",
    "spark.databricks.delta.autoCompact.enabled": "false",
    "spark.databricks.delta.optimizeWrite.enabled": "false",
}

SPARK_JARS = "/opt/airflow/etl/jars/hadoop-aws-3.3.2.jar,/opt/airflow/etl/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/etl/jars/delta-core_2.12-2.3.0.jar,/opt/airflow/etl/jars/delta-storage-2.3.0.jar,/opt/airflow/etl/jars/mysql-connector-java-8.0.28.jar"

with DAG(
    dag_id="Build_ML_Features",
    description="[ML] Build ML dataset from DW",
    default_args=default_args,
    schedule_interval="0 0 * * 0",
    catchup=False,
    tags=["ml", "features", "spark", "delta"],
) as dag:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    build_ml_dataset = SparkSubmitOperator(
        task_id="build_ml_dataset_from_dw",
        conn_id="spark_default",
        application="/opt/airflow/etl/ml_features/build_ml_dataset_from_dw.py",
        name="spark_build_ml_dataset_from_dw",
        executor_cores=2,
        num_executors=2,
        executor_memory="2g",
        driver_memory="512m",
        jars=SPARK_JARS,
        verbose=False,
        conf=SPARK_COMMON_CONF,
    )

    start_task >> build_ml_dataset >> end_task
