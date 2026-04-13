import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

GOLD_DW_BASE = "s3a://lakehouse/gold/dw"
ML_FEATURES_BASE = "s3a://lakehouse/ml_features"

FEATURE_ORDER = [
    "order_features",
    "customer_features",
    "product_features",
    "seller_features",
    "logistics_features",
    "training_datasets",
]


def _dw_path(table_name: str) -> str:
    return f"{GOLD_DW_BASE}/{table_name}"


def _feature_path(table_name: str) -> str:
    return f"{ML_FEATURES_BASE}/{table_name}"


def _read_delta(spark: SparkSession, path: str):
    return spark.read.format("delta").load(path)


def _write_delta_overwrite(df, path: str):
    df.write.format("delta").mode("overwrite").save(path)


def build_order_features(spark: SparkSession):
    fact_sales = _read_delta(spark, _dw_path("fact_sales")) \
        .withColumnRenamed("sale_price", "price") \
        .withColumn("purchase_ts", F.to_timestamp(F.col("purchase_time_key").cast("string"), "yyyyMMdd"))

    fact_payments = _read_delta(spark, _dw_path("fact_payments")) \
        .withColumnRenamed("Total_payment_value", "payment_value")

    fact_reviews = _read_delta(spark, _dw_path("fact_reviews")) \
        .withColumnRenamed("Review_score", "review_score") \
        .withColumnRenamed("Order_id", "order_id")

    # Bù purchase_ts bị thiếu bằng cách Join nhẹ với fact_sales
    fact_shipment = _read_delta(spark, _dw_path("fact_shipment")) \
        .withColumn("delivered_ts", F.to_timestamp(F.col("delivered_customer_date").cast("string"), "yyyyMMdd")) \
        .withColumn("estimated_delivery_ts",
                    F.to_timestamp(F.col("estimated_delivery_date").cast("string"), "yyyyMMdd")) \
        .join(fact_sales.select("order_id", "purchase_ts"), "order_id", "left")

    dim_payment_type = _read_delta(spark, _dw_path("dim_payment_type"))
    dim_customer = _read_delta(spark, _dw_path("dim_customer"))
    dim_product = _read_delta(spark, _dw_path("dim_product"))
    dim_seller = _read_delta(spark, _dw_path("dim_seller"))

    sales_agg = (
        fact_sales.groupBy("order_id")
        .agg(
            F.first("customer_key").alias("customer_key"),
            F.first("product_key").alias("product_key"),
            F.first("seller_key").alias("seller_key"),
            F.sum("price").alias("price"),
            F.sum("freight_value").alias("freight_value"),

            # [SỬA Ở ĐÂY]: Đổi count("order_item_id") thành sum("quantity")
            F.sum("quantity").alias("num_items"),
        )
        .withColumn(
            "freight_ratio",
            F.when(F.col("price") > 0, F.col("freight_value") / F.col("price")).otherwise(F.lit(0.0)),
        )
    )

    payment_agg = (
        fact_payments.alias("fp")
        .join(dim_payment_type.alias("dpt"), "payment_type_key", "left")
        .groupBy("order_id")
        .agg(
            F.first("dpt.payment_type").alias("payment_type"),
            F.avg("payment_installments").alias("payment_installments"),
            F.sum("payment_value").alias("payment_value"),
        )
    )

    review_agg = fact_reviews.groupBy("order_id").agg(F.avg("review_score").alias("review_score"))

    shipment_agg = (
        fact_shipment.groupBy("order_id")
        .agg(
            F.first("purchase_ts").alias("purchase_ts"),
            F.first("delivered_ts").alias("delivered_ts"),
            F.first("estimated_delivery_ts").alias("estimated_delivery_ts"),
        )
        .withColumn("delivery_time", F.datediff("delivered_ts", "purchase_ts"))
        .withColumn("delivery_delay", F.datediff("delivered_ts", "estimated_delivery_ts"))
        .withColumn("is_late_delivery", F.when(F.col("delivery_delay") > 0, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("purchase_hour", F.hour("purchase_ts"))
        .withColumn("purchase_dayofweek", F.dayofweek("purchase_ts"))
    )

    df = (
        sales_agg.alias("s")
        .join(payment_agg.alias("p"), "order_id", "left")
        .join(review_agg.alias("r"), "order_id", "left")
        .join(shipment_agg.alias("sh"), "order_id", "left")
        .join(dim_customer.alias("dc"), F.col("s.customer_key") == F.col("dc.customer_key"), "left")
        .join(dim_product.alias("dp"), F.col("s.product_key") == F.col("dp.product_key"), "left")
        .join(dim_seller.alias("ds"), F.col("s.seller_key") == F.col("ds.seller_key"), "left")
        .select(
            "order_id",
            F.col("dc.customer_id").alias("customer_id"),
            F.col("dp.product_id").alias("product_id"),
            F.col("ds.seller_id").alias("seller_id"),
            "delivery_delay",
            "delivery_time",
            "is_late_delivery",
            "price",
            "freight_value",
            "freight_ratio",
            "num_items",
            "payment_type",
            "payment_installments",
            "payment_value",
            "purchase_hour",
            "purchase_dayofweek",
            "review_score",
            F.col("purchase_ts").cast("date").alias("snapshot_date"),
        )
    )

    _write_delta_overwrite(df, _feature_path("order_features"))


def build_customer_features(spark: SparkSession):
    order_features = _read_delta(spark, _feature_path("order_features"))

    max_date = order_features.agg(F.max("snapshot_date").alias("max_d")).collect()[0]["max_d"]

    if max_date is None:
        raise ValueError("order_features is empty, cannot build customer_features")

    base = order_features.withColumn("days_from_latest", F.datediff(F.lit(max_date), F.col("snapshot_date")))

    orders_30d = (
        base.filter(F.col("days_from_latest") <= 30)
        .groupBy("customer_id")
        .agg(F.countDistinct("order_id").alias("customer_total_orders_30d"))
    )

    orders_90d = (
        base.filter(F.col("days_from_latest") <= 90)
        .groupBy("customer_id")
        .agg(F.countDistinct("order_id").alias("customer_total_orders_90d"))
    )

    agg_all = (
        base.groupBy("customer_id")
        .agg(
            F.avg("payment_value").alias("customer_avg_order_value"),
            F.avg("review_score").alias("customer_avg_review_score"),
            F.avg("is_late_delivery").alias("customer_late_delivery_experience_rate"),
            F.max("snapshot_date").alias("last_order_date"),
        )
        .withColumn("days_since_last_order", F.datediff(F.lit(max_date), F.col("last_order_date")))
        .withColumn("customer_cancel_rate", F.lit(0.0))
        .drop("last_order_date")
    )

    df = (
        agg_all.alias("a")
        .join(orders_30d.alias("o30"), "customer_id", "left")
        .join(orders_90d.alias("o90"), "customer_id", "left")
        .fillna(0, subset=["customer_total_orders_30d", "customer_total_orders_90d"])
    )

    _write_delta_overwrite(df, _feature_path("customer_features"))


def build_product_features(spark: SparkSession):
    order_features = _read_delta(spark, _feature_path("order_features"))

    # Map Lớp DW sang Lớp Logic
    dim_product = _read_delta(spark, _dw_path("dim_product")) \
        .withColumnRenamed("weight", "product_weight_g") \
        .withColumn("product_photos_qty", F.lit(1))  # Giả lập cột hình ảnh bị thiếu ở Lớp Gold

    dim_category = _read_delta(spark, _dw_path("dim_product_category")) \
        .withColumnRenamed("category_name_en", "category_name")

    max_date = order_features.agg(F.max("snapshot_date").alias("max_d")).collect()[0]["max_d"]

    base = order_features.withColumn("days_from_latest", F.datediff(F.lit(max_date), F.col("snapshot_date")))

    sales_30d = (
        base.filter(F.col("days_from_latest") <= 30)
        .groupBy("product_id")
        .agg(F.countDistinct("order_id").alias("product_sales_30d"))
    )

    agg_all = (
        base.groupBy("product_id")
        .agg(
            F.avg("review_score").alias("product_avg_review_score_before_time"),
            F.count("review_score").alias("product_review_count"),
            F.avg("price").alias("product_avg_price"),
            F.avg("freight_ratio").alias("product_avg_freight_ratio"),
        )
        .withColumn("product_return_rate", F.lit(0.0))
    )

    product_enriched = (
        dim_product.alias("dp")
        .join(dim_category.alias("cat"), F.col("dp.category_key") == F.col("cat.category_key"), "left")
        .select(
            "product_id",
            F.col("cat.category_name").alias("product_category"),
            F.col("dp.product_photos_qty").alias("product_photos_qty"),
            F.col("dp.product_weight_g").alias("product_weight"),
        )
    )

    df = (
        agg_all.alias("a")
        .join(sales_30d.alias("s30"), "product_id", "left")
        .join(product_enriched.alias("pe"), "product_id", "left")
        .fillna(0, subset=["product_sales_30d"])
    )

    _write_delta_overwrite(df, _feature_path("product_features"))


def build_seller_features(spark: SparkSession):
    order_features = _read_delta(spark, _feature_path("order_features"))
    dim_seller = _read_delta(spark, _dw_path("dim_seller"))

    fact_sales = _read_delta(spark, _dw_path("fact_sales")) \
        .withColumn("purchase_ts", F.to_timestamp(F.col("purchase_time_key").cast("string"), "yyyyMMdd"))

    fact_shipment = _read_delta(spark, _dw_path("fact_shipment")) \
        .withColumn("delivered_ts", F.to_timestamp(F.col("delivered_customer_date").cast("string"), "yyyyMMdd")) \
        .withColumn("estimated_delivery_ts",
                    F.to_timestamp(F.col("estimated_delivery_date").cast("string"), "yyyyMMdd")) \
        .join(fact_sales.select("order_id", "purchase_ts"), "order_id", "left")

    dim_status = _read_delta(spark, _dw_path("dim_status")) \
        .withColumnRenamed("order_status", "status_name")

    delivery_by_order = (
        fact_shipment.groupBy("order_id", "seller_key")
        .agg(
            F.avg(F.datediff("delivered_ts", "purchase_ts")).alias("delivery_time"),
            F.avg(F.datediff("delivered_ts", "estimated_delivery_ts")).alias("delivery_delay"),
        )
        .withColumn("is_late", F.when(F.col("delivery_delay") > 0, F.lit(1)).otherwise(F.lit(0)))
    )

    status_by_seller = (
        fact_sales.alias("fs")
        .join(dim_status.alias("ds"), "status_key", "left")
        .groupBy("seller_key")
        .agg(
            F.avg(F.when(F.col("ds.status_name") == "canceled", F.lit(1)).otherwise(F.lit(0))).alias(
                "seller_cancel_rate")
        )
    )

    of_agg = (
        order_features.groupBy("seller_id")
        .agg(
            F.countDistinct("order_id").alias("seller_total_orders"),
            F.avg("review_score").alias("seller_avg_review_score"),
        )
    )

    delay_agg = (
        delivery_by_order.alias("dbo")
        .join(dim_seller.alias("dsel"), "seller_key", "left")
        .groupBy("dsel.seller_id")
        .agg(
            F.avg("delivery_delay").alias("seller_avg_delivery_delay"),
            F.avg("is_late").alias("seller_late_rate"),
        )
    )

    seller_state = dim_seller.select("seller_id", "city_key").withColumnRenamed("city_key", "seller_state")

    cancel_agg = (
        status_by_seller.alias("ss")
        .join(dim_seller.alias("dsel"), "seller_key", "left")
        .groupBy("dsel.seller_id")
        .agg(F.avg("seller_cancel_rate").alias("seller_cancel_rate"))
    )

    df = (
        of_agg.alias("o")
        .join(delay_agg.alias("d"), "seller_id", "left")
        .join(cancel_agg.alias("c"), "seller_id", "left")
        .join(seller_state.alias("s"), "seller_id", "left")
        .fillna(0.0, subset=["seller_avg_delivery_delay", "seller_late_rate", "seller_cancel_rate"])
    )

    _write_delta_overwrite(df, _feature_path("seller_features"))


def build_logistics_features(spark: SparkSession):
    dim_customer = _read_delta(spark, _dw_path("dim_customer"))
    dim_seller = _read_delta(spark, _dw_path("dim_seller"))
    dim_geo = _read_delta(spark, _dw_path("dim_geolocation"))

    fact_sales = _read_delta(spark, _dw_path("fact_sales")) \
        .withColumn("purchase_ts", F.to_timestamp(F.col("purchase_time_key").cast("string"), "yyyyMMdd"))

    fact_shipment = _read_delta(spark, _dw_path("fact_shipment")) \
        .withColumn("delivered_ts", F.to_timestamp(F.col("delivered_customer_date").cast("string"), "yyyyMMdd")) \
        .withColumn("estimated_delivery_ts",
                    F.to_timestamp(F.col("estimated_delivery_date").cast("string"), "yyyyMMdd")) \
        .withColumn("carrier_ts", F.to_timestamp(F.col("delivered_carrier_date").cast("string"), "yyyyMMdd")) \
        .join(fact_sales.select("order_id", "purchase_ts"), "order_id", "left")

    df = (
        fact_shipment.alias("fs")
        .join(dim_customer.alias("dc"), "customer_key", "left")
        .join(dim_seller.alias("ds"), "seller_key", "left")
        .join(dim_geo.alias("geo_cust"), F.col("dc.city_key") == F.col("geo_cust.geolocation_key"), "left")
        .join(dim_geo.alias("geo_seller"), F.col("ds.city_key") == F.col("geo_seller.geolocation_key"), "left")
        .select(
            "order_id",
            F.datediff("delivered_ts", "estimated_delivery_ts").alias("delivery_delay"),
            F.datediff("delivered_ts", "purchase_ts").alias("delivery_time"),
            F.datediff("delivered_ts", "carrier_ts").alias("carrier_to_customer_days"),

            (
                F.asin(F.sqrt(
                    F.pow(F.sin(F.radians(F.col("geo_seller.lat") - F.col("geo_cust.lat")) / 2), 2) +
                    F.cos(F.radians(F.col("geo_cust.lat"))) * F.cos(F.radians(F.col("geo_seller.lat"))) *
                    F.pow(F.sin(F.radians(F.col("geo_seller.lng") - F.col("geo_cust.lng")) / 2), 2)
                )) * 2 * 6371
            ).alias("shipping_distance_km")
        )
    )

    _write_delta_overwrite(df, _feature_path("logistics_features"))


def build_training_datasets(spark: SparkSession):
    order_features = _read_delta(spark, _feature_path("order_features"))
    customer_features = _read_delta(spark, _feature_path("customer_features"))
    product_features = _read_delta(spark, _feature_path("product_features"))
    seller_features = _read_delta(spark, _feature_path("seller_features"))
    logistics_features = _read_delta(spark, _feature_path("logistics_features"))

    df = (
        order_features.alias("o")
        .join(customer_features.alias("c"), "customer_id", "left")
        .join(product_features.alias("p"), "product_id", "left")
        .join(seller_features.alias("s"), "seller_id", "left")
        .join(logistics_features.alias("l"), "order_id", "left") \
        .drop("delivery_delay", "delivery_time") \
        .withColumn(
            "review_score_binary",
            F.when(F.col("review_score") >= 4, F.lit(1)).otherwise(F.lit(0)),
        )
    )

    _write_delta_overwrite(df, _feature_path("training_datasets"))


BUILDERS = {
    "order_features": build_order_features,
    "customer_features": build_customer_features,
    "product_features": build_product_features,
    "seller_features": build_seller_features,
    "logistics_features": build_logistics_features,
    "training_datasets": build_training_datasets,
}


def process_feature_table(spark: SparkSession, table_name: str):
    if table_name not in BUILDERS:
        raise ValueError(f"Unsupported feature table: {table_name}")
    logger.info("Building feature table %s", table_name)
    BUILDERS[table_name](spark)