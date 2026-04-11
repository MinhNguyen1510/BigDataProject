import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

GOLD_DW_BASE = "s3a://lakehouse/gold/dw"
GOLD_MART_BASE = "s3a://lakehouse/gold/mart"

MART_ORDER = [
    "revenue_mart",
    "cashflow_mart",
    "customer_experience_mart",
    "logistics_mart",
]


def _dw_path(table_name: str) -> str:
    return f"{GOLD_DW_BASE}/{table_name}"


def _mart_path(table_name: str) -> str:
    return f"{GOLD_MART_BASE}/{table_name}"


def _read_delta(spark: SparkSession, path: str):
    return spark.read.format("delta").load(path)


def _write_delta_overwrite(df, path: str):
    df.write.format("delta").mode("overwrite").save(path)


def build_revenue_mart(spark: SparkSession):
    fact_sales = _read_delta(spark, _dw_path("fact_sales")) \
        .withColumnRenamed("sale_price", "price")

    dim_product = _read_delta(spark, _dw_path("dim_product"))

    dim_customer = _read_delta(spark, _dw_path("dim_customer")) \
        .withColumnRenamed("city_key", "geolocation_key")

    dim_seller = _read_delta(spark, _dw_path("dim_seller"))

    dim_time = _read_delta(spark, _dw_path("dim_time")) \
        .withColumnRenamed("full_date", "date")

    dim_category = _read_delta(spark, _dw_path("dim_product_category")) \
        .withColumnRenamed("category_name_en", "category_name")

    df = (
        fact_sales.alias("fs")
        .join(dim_product.alias("dp"), "product_key", "left")
        .join(dim_customer.alias("dc"), "customer_key", "left")
        .join(dim_seller.alias("ds"), "seller_key", "left")
        # [Chỉnh lại một chút đường nối cho đúng chuẩn DW mới]
        .join(dim_time.alias("dt"), F.col("fs.purchase_time_key") == F.col("dt.time_key"), "left")
        .join(dim_category.alias("cat"), F.col("dp.category_key") == F.col("cat.category_key"), "left")
        .groupBy(
            F.col("dt.date").alias("date"),
            F.col("cat.category_name").alias("product_category"),
            F.col("ds.seller_key").alias("seller_key"),
            F.col("dc.geolocation_key").alias("customer_geo_key"),
        )
        .agg(
            F.countDistinct("fs.order_id").alias("total_orders"),
            F.sum(F.col("fs.price") + F.col("fs.freight_value")).alias("total_revenue"),
            F.avg(F.col("fs.price") + F.col("fs.freight_value")).alias("avg_order_value"),
            # Sửa count(*) thành sum(quantity) như đã fix ở ML Features
            F.sum("fs.quantity").alias("total_items_sold"),
            F.avg("fs.price").alias("avg_price"),
            F.avg("fs.freight_value").alias("avg_freight_value"),
        )
    )

    _write_delta_overwrite(df, _mart_path("revenue_mart"))


def build_cashflow_mart(spark: SparkSession):
    fact_payments = _read_delta(spark, _dw_path("fact_payments")) \
        .withColumnRenamed("Total_payment_value", "payment_value")

    dim_payment_type = _read_delta(spark, _dw_path("dim_payment_type"))

    dim_time = _read_delta(spark, _dw_path("dim_time")) \
        .withColumnRenamed("full_date", "date")

    df = (
        fact_payments.alias("fp")
        .join(dim_payment_type.alias("dpt"), "payment_type_key", "left")
        .join(dim_time.alias("dt"), "time_key", "left")
        .groupBy(
            F.col("dt.date").alias("date"),
            F.col("dpt.payment_type").alias("payment_type"),
        )
        .agg(
            F.sum("fp.payment_value").alias("total_payment_value"),
            F.avg("fp.payment_value").alias("avg_payment_value"),
            F.avg("fp.payment_installments").alias("avg_installments"),
            F.avg(F.when(F.col("fp.payment_installments") > 1, F.lit(1)).otherwise(F.lit(0))).alias(
                "installment_ratio"),
            F.avg(F.when(F.col("dpt.payment_type") == "voucher", F.lit(1)).otherwise(F.lit(0))).alias(
                "voucher_usage_rate"),
        )
    )

    _write_delta_overwrite(df, _mart_path("cashflow_mart"))


def build_customer_experience_mart(spark: SparkSession):
    fact_reviews = _read_delta(spark, _dw_path("fact_reviews")) \
        .withColumnRenamed("Review_score", "review_score") \
        .withColumnRenamed("Order_id", "order_id")

    fact_sales = _read_delta(spark, _dw_path("fact_sales")) \
        .withColumnRenamed("sale_price", "price")

    dim_time = _read_delta(spark, _dw_path("dim_time")) \
        .withColumnRenamed("full_date", "date")

    dim_product = _read_delta(spark, _dw_path("dim_product"))

    dim_category = _read_delta(spark, _dw_path("dim_product_category")) \
        .withColumnRenamed("category_name_en", "category_name")

    dim_seller = _read_delta(spark, _dw_path("dim_seller")) \
        .withColumnRenamed("city_key", "geolocation_key")

    df = (
        fact_reviews.alias("fr")
        .join(fact_sales.alias("fs"), ["order_id"], "left")
        .join(dim_time.alias("dt"), "time_key", "left")
        .join(dim_product.alias("dp"), "product_key", "left")
        .join(dim_category.alias("cat"), F.col("dp.category_key") == F.col("cat.category_key"), "left")
        .join(dim_seller.alias("ds"), "seller_key", "left")
        .groupBy(
            F.col("dt.date").alias("date"),
            F.col("cat.category_name").alias("product_category"),
            F.col("ds.geolocation_key").alias("seller_geo_key"),
        )
        .agg(
            F.avg("fr.review_score").alias("avg_review_score"),
            F.avg(F.when(F.col("fr.review_score") >= 4, F.lit(1)).otherwise(F.lit(0))).alias("good_review_rate"),
            F.avg(F.when(F.col("fr.review_score") <= 2, F.lit(1)).otherwise(F.lit(0))).alias("bad_review_rate"),
            F.avg(F.lit(0)).alias("avg_delivery_delay"),
            F.avg(F.lit(0)).alias("late_delivery_rate"),
            F.avg("fs.freight_value").alias("avg_freight_value"),
            F.avg("fs.price").alias("avg_price"),
            F.count("*").alias("review_volume"),
        )
    )

    _write_delta_overwrite(df, _mart_path("customer_experience_mart"))


def build_logistics_mart(spark: SparkSession):
    fact_sales = _read_delta(spark, _dw_path("fact_sales")) \
        .withColumn("purchase_ts", F.to_timestamp(F.col("purchase_time_key").cast("string"), "yyyyMMdd"))

    fact_shipment = _read_delta(spark, _dw_path("fact_shipment")) \
        .withColumn("delivered_ts", F.to_timestamp(F.col("delivered_customer_date").cast("string"), "yyyyMMdd")) \
        .withColumn("estimated_delivery_ts",
                    F.to_timestamp(F.col("estimated_delivery_date").cast("string"), "yyyyMMdd")) \
        .join(fact_sales.select("order_id", "purchase_ts"), "order_id", "left")

    dim_customer = _read_delta(spark, _dw_path("dim_customer")) \
        .withColumnRenamed("city_key", "geolocation_key")

    dim_seller = _read_delta(spark, _dw_path("dim_seller")) \
        .withColumnRenamed("city_key", "geolocation_key")

    dim_time = _read_delta(spark, _dw_path("dim_time"))

    df = (
        fact_shipment.alias("fsh")
        .join(dim_customer.alias("dc"), "customer_key", "left")
        .join(dim_seller.alias("ds"), "seller_key", "left")
        .withColumn("delivery_time", F.datediff("delivered_ts", "purchase_ts"))
        .withColumn("delivery_delay", F.datediff("delivered_ts", "estimated_delivery_ts"))
        .groupBy(
            F.col("ds.geolocation_key").alias("seller_geo_key"),
            F.col("dc.geolocation_key").alias("customer_geo_key"),
            F.col("purchase_ts").cast("date").alias("date"),
        )
        .agg(
            F.avg("delivery_time").alias("avg_delivery_time"),
            F.avg("delivery_delay").alias("avg_delivery_delay"),
            F.avg(F.when(F.col("delivery_delay") > 0, F.lit(1)).otherwise(F.lit(0))).alias("late_delivery_rate"),
            F.avg(F.when(F.col("delivery_delay") <= 0, F.lit(1)).otherwise(F.lit(0))).alias("on_time_rate"),
            F.avg(F.datediff("delivered_ts", "purchase_ts")).alias("avg_shipping_duration"),
            F.avg(F.datediff("delivered_ts", "purchase_ts")).alias("carrier_to_customer_days"),
            F.count("*").alias("order_volume"),
        )
    )

    _write_delta_overwrite(df, _mart_path("logistics_mart"))


BUILDERS = {
    "revenue_mart": build_revenue_mart,
    "cashflow_mart": build_cashflow_mart,
    "customer_experience_mart": build_customer_experience_mart,
    "logistics_mart": build_logistics_mart,
}


def process_mart_table(spark: SparkSession, table_name: str):
    if table_name not in BUILDERS:
        raise ValueError(f"Unsupported mart table: {table_name}")
    logger.info("Building mart table %s", table_name)
    BUILDERS[table_name](spark)