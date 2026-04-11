import argparse
import logging
import os
from pyspark.sql import SparkSession

from etl.ml_features.feature_processor import process_feature_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_spark_session(app_name: str = "ML_Features_Processing") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ML Features Processor")
    parser.add_argument("--table_name", required=True, help="Ten bang feature can build")
    args = parser.parse_args()

    table_name = args.table_name
    logger.info("Start feature table: %s", table_name)

    spark = build_spark_session(f"ML_Features_{table_name}")
    try:
        process_feature_table(spark, table_name)
        logger.info("Done feature table: %s", table_name)
    except Exception as exc:
        logger.exception("ML features failed for %s", table_name)
        raise exc
    finally:
        spark.stop()