import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, to_timestamp, lit, expr
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SILVER_PATH = "s3a://lakehouse/silver/clean_"
GOLD_PATH = "s3a://lakehouse/gold/dw/"

def apply_scd2(spark, df_new, gold_path, table_name, business_key, surrogate_key_name, track_cols):
    spark.sql("CREATE DATABASE IF NOT EXISTS dw_db LOCATION 's3a://lakehouse/gold/dw/'")
    full_table_name = f"dw_db.{table_name}"
    if not DeltaTable.isDeltaTable(spark, gold_path):
        logger.info(f"[{full_table_name}] Bảng chưa có -> Initial Load SCD 2")
        df_new.withColumn(surrogate_key_name, md5(expr(f"concat({business_key}, current_timestamp())"))) \
            .withColumn("effective_date", to_timestamp(lit("1900-01-01 00:00:00"))) \
            .withColumn("expiry_date", expr("CAST(NULL AS TIMESTAMP)")) \
            .withColumn("is_current", lit(1)) \
            .write.format("delta") \
            .option("path", gold_path) \
            .saveAsTable(full_table_name)
        return

    logger.info(f"[{full_table_name}] Bảng đã có -> MERGE SCD 2")
    target_table = DeltaTable.forName(spark, full_table_name)
    change_conditions = " OR ".join([f"target.{c} != new.{c}" for c in track_cols])
    where_clause = f"target.is_current = 1 AND ({change_conditions})"

    updates_df = df_new.alias("new").join(target_table.toDF().alias("target"), business_key).where(where_clause).selectExpr("NULL as merge_key", "new.*")
    staged_updates = updates_df.unionByName(df_new.selectExpr(f"{business_key} as merge_key", "*"))

    insert_values = {
        surrogate_key_name: f"md5(concat(source.{business_key}, current_timestamp()))",
        "effective_date": "current_timestamp()",
        "expiry_date": "CAST(NULL AS TIMESTAMP)",
        "is_current": "1"
    }
    for c in df_new.columns:
        insert_values[c] = f"source.{c}"

    target_table.alias("target").merge(
        staged_updates.alias("source"), f"target.{business_key} = source.merge_key"
    ).whenMatchedUpdate(
        condition=f"target.is_current = 1 AND ({change_conditions.replace('new.', 'source.')})",
        set={"is_current": "0", "expiry_date": "current_timestamp()"}
    ).whenNotMatchedInsert(values=insert_values).execute()

def get_spark_session(app_name):
    spark = (SparkSession.builder.appName(app_name)
             .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
             .config("spark.hadoop.fs.s3a.access.key", "minio")
             .config("spark.hadoop.fs.s3a.secret.key", "minio123")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
             .config("spark.hadoop.fs.s3a.fast.upload", "true")
             .config("spark.sql.catalogImplementation", "hive")
             .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark