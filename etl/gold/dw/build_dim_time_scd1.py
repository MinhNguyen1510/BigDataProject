from pyspark.sql.functions import col, md5, date_format, when, first, explode
from common_utils import get_spark_session, SILVER_PATH, GOLD_PATH, logger

if __name__ == "__main__":
    spark = get_spark_session("Build_Dim_Time_And_SCD1")
    logger.info("--- Đang xử lý các bảng DIMENSION Nhỏ (SCD1) & TIME ---")

    df_geo = spark.read.format("delta").load(SILVER_PATH + "geolocation/")
    df_category = spark.read.format("delta").load(SILVER_PATH + "product_category_name_translation/")
    df_orders = spark.read.format("delta").load(SILVER_PATH + "orders/")
    df_payments = spark.read.format("delta").load(SILVER_PATH + "order_payments/")

    dim_geolocation = df_geo.groupBy("geolocation_zip_code_prefix").agg(
        first("geolocation_lat").alias("lat"), first("geolocation_lng").alias("lng"),
        first("geolocation_city").alias("city"), first("geolocation_state").alias("state")
    ).withColumn("geolocation_key", md5(col("geolocation_zip_code_prefix"))).withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix").select("geolocation_key", "zip_code_prefix", "lat", "lng", "city", "state")

    dim_category = df_category.withColumn("category_key", md5(col("product_category_name"))).withColumnRenamed("product_category_name", "category_name_pt").withColumnRenamed("product_category_name_english", "category_name_en").select("category_key", "category_name_pt", "category_name_en")
    dim_status = df_orders.select("order_status").distinct().withColumn("status_key", md5(col("order_status"))).select("status_key", "order_status")
    dim_payment_type = df_payments.select("payment_type").distinct().withColumn("payment_type_key", md5(col("payment_type"))).select("payment_type_key", "payment_type")

    dim_time = spark.sql("SELECT explode(sequence(to_date('2008-01-01'), to_date('2030-12-31'), interval 1 day)) as full_date") \
        .withColumn("time_key", date_format(col("full_date"), "yyyyMMdd").cast("int")).withColumn("day", date_format(col("full_date"), "dd").cast("int")).withColumn("month", date_format(col("full_date"), "MM").cast("int")).withColumn("quarter", date_format(col("full_date"), "q").cast("int")).withColumn("year", date_format(col("full_date"), "yyyy").cast("int")).withColumn("day_of_week", date_format(col("full_date"), "E")).withColumn("is_weekend", when(date_format(col("full_date"), "E").isin("Sat", "Sun"), 1).otherwise(0)).select("time_key", "full_date", "day", "month", "quarter", "year", "day_of_week", "is_weekend")

    for name, df in {"dim_geolocation": dim_geolocation, "dim_product_category": dim_category, "dim_status": dim_status, "dim_payment_type": dim_payment_type, "dim_time": dim_time}.items():
        df.write.format("delta").mode("overwrite").save(GOLD_PATH + f"{name}/")
        logger.info(f" [+] Đã lưu {name}")
    spark.stop()