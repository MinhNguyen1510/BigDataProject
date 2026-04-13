from pyspark.sql.functions import col, md5, date_format, first, expr
from common_utils import get_spark_session, SILVER_PATH, GOLD_PATH, logger
from delta.tables import DeltaTable

if __name__ == "__main__":
    spark = get_spark_session("Build_Fact_Reviews")
    df_orders = spark.read.format("delta").load(SILVER_PATH + "orders/")
    df_items = spark.read.format("delta").load(SILVER_PATH + "order_items/")
    df_reviews = spark.read.format("delta").load(SILVER_PATH + "order_reviews/")
    gold_dim_cust = spark.read.format("delta").load(GOLD_PATH + "dim_customer/")
    gold_dim_prod = spark.read.format("delta").load(GOLD_PATH + "dim_product/")

    fact_reviews = df_reviews.alias("r").join(df_orders.alias("o"), "order_id", "left").join(df_items.groupBy("order_id").agg(first("product_id").alias("product_id")).alias("i"), "order_id", "left") \
        .join(gold_dim_cust.alias("c"), expr("o.customer_id = c.customer_id AND r.review_creation_date >= c.effective_date AND (r.review_creation_date < c.expiry_date OR c.expiry_date IS NULL)"), "left") \
        .join(gold_dim_prod.alias("p"), expr("i.product_id = p.product_id AND r.review_creation_date >= p.effective_date AND (r.review_creation_date < p.expiry_date OR p.expiry_date IS NULL)"), "left") \
        .withColumn("time_key", date_format(col("r.review_creation_date"), "yyyyMMdd").cast("int")) \
        .select(col("r.review_id").alias("Review_id"), col("r.order_id").alias("Order_id"), "c.customer_key", "p.product_key", col("r.review_score").alias("Review_score"), "time_key")

    target_path = GOLD_PATH + "fact_reviews/"
    spark.sql("CREATE DATABASE IF NOT EXISTS dw_db LOCATION 's3a://lakehouse/gold/dw/'")

    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forName(spark, "dw_db.fact_reviews")
        logger.info(" [*] Tiến hành MERGE (Incremental Load) bảng fact_reviews...")

        (delta_table.alias("t")
         # Khóa chính của bảng Review thường là Review_id (nhưng kèm Order_id cho chắc chắn)
         .merge(fact_reviews.alias("s"), "t.Review_id = s.Review_id AND t.Order_id = s.Order_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

        logger.info(" [+] Đã MERGE xong fact_reviews")
    else:
        fact_reviews.write.format("delta").mode("overwrite") \
            .option("path", target_path) \
            .saveAsTable("dw_db.fact_reviews")
        logger.info(" [+] Khởi tạo dw_db.fact_reviews")

    spark.stop()