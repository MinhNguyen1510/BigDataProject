import os
import sys
from pyspark.sql import functions as F

# Ensure DW utils module is importable when executed via spark-submit.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DW_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "gold", "dw"))
if DW_DIR not in sys.path:
    sys.path.insert(0, DW_DIR)

from common_utils import get_spark_session, GOLD_PATH


def main() -> None:
    spark = get_spark_session("Build_ML_Dataset_From_DW")

    fact_sales = spark.read.format("delta").load(GOLD_PATH + "fact_sales/")
    fact_reviews = spark.read.format("delta").load(GOLD_PATH + "fact_reviews/")

    # Aggregate order-level features from fact_sales
    sales_agg = (
        fact_sales.groupBy("order_id")
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

    # Convert yyyymmdd int keys to date for delivery_time calculation
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

    # Build label from fact_reviews
    reviews = fact_reviews.select(
        F.col("Order_id").alias("order_id"),
        F.col("Review_score").alias("review_score"),
    )

    df = reviews.join(sales_agg, "order_id", "inner")

    df = df.withColumn("label", (F.col("review_score") >= F.lit(4)).cast("int"))
    df = df.withColumn("freight_ratio", F.col("freight_sum") / F.col("price_sum"))
    df = df.withColumn("is_high_freight", (F.col("freight_ratio") > F.lit(0.5)).cast("int"))
    df = df.withColumn("price_per_item", F.col("price_sum") / F.col("item_count"))
    df = df.withColumn("delay_ratio", F.col("delivery_delay") / (F.col("delivery_time") + F.lit(1)))

    # Drop rows missing required features
    df = df.dropna(
        subset=[
            "review_score",
            "delivery_delay",
            "delivery_time",
            "price_sum",
            "freight_sum",
        ]
    )

    final_cols = [
        "order_id",
        "review_score",
        "label",
        "delivery_delay",
        "delivery_time",
        "price_sum",
        "price_mean",
        "freight_sum",
        "freight_mean",
        "freight_ratio",
        "is_high_freight",
        "price_per_item",
        "delay_ratio",
        "item_count",
    ]
    df = df.select(*final_cols)

    output_path = "s3a://lakehouse/features_ml/olist_ml_dataset/"
    df.write.format("delta").mode("overwrite").save(output_path)

    print(f"Wrote dataset to {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
