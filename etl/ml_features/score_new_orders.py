import os
import sys
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from delta.tables import DeltaTable

# Ensure DW utils module is importable when executed via spark-submit.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DW_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "gold", "dw"))
if DW_DIR not in sys.path:
    sys.path.insert(0, DW_DIR)

from common_utils import get_spark_session, GOLD_PATH

FEATURE_COLS = [
    "delivery_delay",
    "delivery_time",
    "price_sum",
    "price_mean",
    "freight_sum",
    "freight_mean",
    "freight_ratio",
    "item_count",
    "is_high_freight",
    "price_per_item",
    "delay_ratio",
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--features-data",
        default="s3a://lakehouse/features_ml/olist_ml_dataset/",
        help="Delta dataset path for mean imputation",
    )
    parser.add_argument(
        "--model",
        default="s3a://lakehouse/models/olist_logreg",
        help="Spark MLlib model path",
    )
    parser.add_argument(
        "--alerts-path",
        default="s3a://lakehouse/alerts/low_rating_orders/",
        help="Delta path for alert outputs",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.55,
        help="Prediction threshold for low rating alerts",
    )
    return parser.parse_args()


def load_production_model(model_uri: str) -> PipelineModel:
    return PipelineModel.load(model_uri)


def build_features(fact_sales_df):
    sales_agg = (
        fact_sales_df.groupBy("order_id")
        .agg(
            F.sum(F.col("sale_price") * F.col("quantity")).alias("price_sum"),
            F.avg("sale_price").alias("price_mean"),
            F.sum("freight_value").alias("freight_sum"),
            F.avg("freight_value").alias("freight_mean"),
            F.sum("quantity").alias("item_count"),
            F.first("delivery_delay_days").alias("delivery_delay"),
            F.first("purchase_time_key").alias("purchase_time_key"),
            F.first("delivery_time_key").alias("delivery_time_key"),
        )
    )

    sales_agg = sales_agg.withColumn(
        "purchase_date",
        F.to_date(F.col("purchase_time_key").cast("string"), "yyyyMMdd"),
    ).withColumn(
        "delivery_date",
        F.to_date(F.col("delivery_time_key").cast("string"), "yyyyMMdd"),
    )

    sales_agg = sales_agg.withColumn(
        "delivery_time", F.datediff(F.col("delivery_date"), F.col("purchase_date"))
    )

    sales_agg = sales_agg.withColumn(
        "freight_ratio", F.col("freight_sum") / F.col("price_sum")
    )
    sales_agg = sales_agg.withColumn(
        "is_high_freight", (F.col("freight_ratio") > F.lit(0.5)).cast("double")
    )
    sales_agg = sales_agg.withColumn(
        "price_per_item", F.col("price_sum") / F.col("item_count")
    )
    sales_agg = sales_agg.withColumn(
        "delay_ratio", F.col("delivery_delay") / (F.col("delivery_time") + F.lit(1))
    )

    return sales_agg


def main() -> None:
    args = parse_args()
    spark = get_spark_session("Score_New_Orders")
    spark.sparkContext.setLogLevel("ERROR")

    fact_sales = spark.read.format("delta").load(GOLD_PATH + "fact_sales/")
    features = build_features(fact_sales)

    alerts_path = args.alerts_path
    if DeltaTable.isDeltaTable(spark, alerts_path):
        existing_alerts = spark.read.format("delta").load(alerts_path)
        max_time_key = (
            existing_alerts.agg(F.max("purchase_time_key").alias("max_key")).collect()[0]["max_key"]
        )
        if max_time_key is not None:
            features = features.filter(F.col("purchase_time_key") > F.lit(max_time_key))

        features = features.join(
            existing_alerts.select("order_id").dropDuplicates(["order_id"]),
            "order_id",
            "left_anti",
        )
    else:
        latest_key = (
            features.agg(F.max("purchase_time_key").alias("max_key")).collect()[0]["max_key"]
        )
        if latest_key is not None:
            features = features.filter(F.col("purchase_time_key") == F.lit(latest_key))

    means_df = spark.read.format("delta").load(args.features_data)
    means_row = means_df.select([F.mean(F.col(c)).alias(c) for c in FEATURE_COLS]).collect()[0]
    means = {c: means_row[c] for c in FEATURE_COLS}

    for col_name in FEATURE_COLS:
        default_value = means.get(col_name, 0.0) if means.get(col_name) is not None else 0.0
        features = features.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(default_value)))

    model = load_production_model(args.model)
    pred = model.transform(features)
    pred = pred.withColumn("score", vector_to_array("probability")[1])

    alerts = pred.filter(F.col("score") < F.lit(args.threshold)).select(
        "order_id",
        "purchase_time_key",
        F.col("score").cast(DoubleType()).alias("score"),
        F.lit(0).alias("predicted_label"),
        F.current_timestamp().alias("scored_at"),
    )

    alerts.write.format("delta").mode("append").save(alerts_path)

    print("Scoring done.")
    spark.stop()


if __name__ == "__main__":
    main()
