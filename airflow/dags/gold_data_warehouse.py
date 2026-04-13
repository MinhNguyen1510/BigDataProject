from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "start_date": datetime(2026, 4, 12),
}

with DAG(
        dag_id="Gold_Data_Warehouse",
        description="[Gold] DW - Tách bảng chạy nối tiếp siêu tiết kiệm RAM",
        default_args=default_args,
        schedule_interval="0 2 * * *",
        catchup=False,
        tags=["gold", "spark", "delta", "optimized"],
) as dag:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    spark_conf_base = {
        "spark.sql.catalogImplementation": "hive",
        "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.autoBroadcastJoinThreshold": "10485760",
        "spark.sql.join.preferSortMergeJoin": "true",
        "spark.executor.memoryOverhead": "256m",
        "spark.driver.memoryOverhead": "256m",
        "spark.cores.max": "2",
        "spark.sql.shuffle.partitions": "2",
        "spark.databricks.delta.autoCompact.enabled": "false",
        "spark.databricks.delta.optimizeWrite.enabled": "false"
    }


    def create_spark_task(task_id, script_name):
        return SparkSubmitOperator(
            task_id=task_id,
            conn_id="spark_default",
            application=f"/opt/airflow/etl/gold/dw/{script_name}",
            name=f"gold_{task_id}",
            executor_cores=2,
            num_executors=2,
            executor_memory="2g",
            driver_memory="512m",
            jars="/opt/airflow/etl/jars/hadoop-aws-3.3.2.jar,/opt/airflow/etl/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/etl/jars/delta-core_2.12-2.3.0.jar,/opt/airflow/etl/jars/delta-storage-2.3.0.jar",
            verbose=False,
            conf=spark_conf_base
        )


    # Khởi tạo các Task
    t_dim_scd1 = create_spark_task("build_dim_time_scd1", "build_dim_time_scd1.py")
    t_dim_cust = create_spark_task("build_dim_customer", "build_dim_customer.py")
    t_dim_seller = create_spark_task("build_dim_seller", "build_dim_seller.py")
    t_dim_prod = create_spark_task("build_dim_product", "build_dim_product.py")

    t_fact_sales = create_spark_task("build_fact_sales", "build_fact_sales.py")
    t_fact_reviews = create_spark_task("build_fact_reviews", "build_fact_reviews.py")
    t_fact_payments = create_spark_task("build_fact_payments", "build_fact_payments.py")
    t_fact_shipment = create_spark_task("build_fact_shipment", "build_fact_shipment.py")

    start_task >> t_dim_scd1 >> t_dim_cust >> t_dim_seller >> t_dim_prod \
    >> t_fact_sales >> t_fact_reviews >> t_fact_payments >> t_fact_shipment >> end_task