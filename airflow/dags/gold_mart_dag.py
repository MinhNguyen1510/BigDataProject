from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain

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
    "spark.databricks.delta.optimizeWrite.enabled": "false"
}

SPARK_JARS = "/opt/airflow/etl/jars/hadoop-aws-3.3.2.jar,/opt/airflow/etl/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/etl/jars/delta-core_2.12-2.3.0.jar,/opt/airflow/etl/jars/delta-storage-2.3.0.jar,/opt/airflow/etl/jars/mysql-connector-java-8.0.28.jar"

with DAG(
        dag_id="Gold_Mart",
        description="[Gold] Data Mart & ML Features (Chạy sau DW lúc 3AM)",
        default_args=default_args,
        schedule_interval="0 3 * * *",
        catchup=False,
        tags=["gold", "mart", "ml_features", "spark", "delta"],
) as dag:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")


    def create_spark_task(task_id, app_path, table_name):
        return SparkSubmitOperator(
            task_id=task_id,
            conn_id="spark_default",
            application=app_path,
            name=f"spark_{task_id}",
            executor_cores=2,
            num_executors=2,
            executor_memory="2g",
            driver_memory="512m",
            application_args=["--table_name", table_name],
            jars=SPARK_JARS,
            verbose=False,
            conf=SPARK_COMMON_CONF
        )


    with TaskGroup("Data_Marts") as tg_marts:
        marts = ["revenue_mart", "cashflow_mart", "customer_experience_mart", "logistics_mart"]

        mart_tasks = [create_spark_task(f"build_{m}", "/opt/airflow/etl/gold/mart/main_mart.py", m) for m in marts]

        chain(*mart_tasks)

    start_task >> tg_marts >> end_task