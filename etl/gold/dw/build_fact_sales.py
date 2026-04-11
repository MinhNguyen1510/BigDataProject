from pyspark.sql.functions import col, md5, date_format, when, datediff, count, sum as _sum, first, expr, round
from common_utils import get_spark_session, SILVER_PATH, GOLD_PATH, logger
from delta.tables import DeltaTable

if __name__ == "__main__":
    spark = get_spark_session("Build_Fact_Sales")
    df_orders = spark.read.format("delta").load(SILVER_PATH + "orders/")
    df_items = spark.read.format("delta").load(SILVER_PATH + "order_items/")
    gold_dim_cust = spark.read.format("delta").load(GOLD_PATH + "dim_customer/")
    gold_dim_seller = spark.read.format("delta").load(GOLD_PATH + "dim_seller/")
    gold_dim_prod = spark.read.format("delta").load(GOLD_PATH + "dim_product/")

    df_items_grouped = df_items.groupBy("order_id", "product_id", "seller_id").agg(count("order_item_id").alias("quantity"), round(first("price"), 2).alias("sale_price"), round(_sum("freight_value"), 2).alias("freight_value"))

    fact_sales = (df_items_grouped.alias("i").join(df_orders.alias("o"), "order_id", "inner") \
        .join(gold_dim_cust.alias("c"), expr("o.customer_id = c.customer_id AND o.order_purchase_timestamp >= c.effective_date AND (o.order_purchase_timestamp < c.expiry_date OR c.expiry_date IS NULL)"), "left") \
        .join(gold_dim_seller.alias("s"), expr("i.seller_id = s.seller_id AND o.order_purchase_timestamp >= s.effective_date AND (o.order_purchase_timestamp < s.expiry_date OR s.expiry_date IS NULL)"), "left") \
        .join(gold_dim_prod.alias("p"), expr("i.product_id = p.product_id AND o.order_purchase_timestamp >= p.effective_date AND (o.order_purchase_timestamp < p.expiry_date OR p.expiry_date IS NULL)"), "left") \
        .withColumn("purchase_time_key", date_format(col("o.order_purchase_timestamp"), "yyyyMMdd").cast("int"))
        .withColumn("delivery_time_key", date_format(col("o.order_delivered_customer_date"), "yyyyMMdd").cast("int"))
        .withColumn("status_key", md5(col("o.order_status")))
        .withColumn("total_value", round(col("i.sale_price") * col("i.quantity"), 2))
        .withColumn("is_canceled", when(col("o.order_status") == 'canceled', 1).otherwise(0))
        .withColumn("delivery_delay_days", datediff(col("o.order_delivered_customer_date"), col("o.order_estimated_delivery_date"))) \
        .select("o.order_id", "c.customer_key", "p.product_key", "s.seller_key", "purchase_time_key", "delivery_time_key", "status_key", "i.sale_price", "i.quantity", "total_value", "is_canceled", "delivery_delay_days", "i.freight_value"))

    target_path = GOLD_PATH + "fact_sales/"

    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        logger.info(" [*] Tiến hành MERGE (Incremental Load) bảng fact_sales...")

        (delta_table.alias("t")
         .merge(fact_sales.alias("s"),
                "t.order_id = s.order_id AND t.product_key = s.product_key AND t.seller_key = s.seller_key")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

        logger.info(" [+] Đã MERGE xong fact_sales")
    else:
        fact_sales.write.format("delta").mode("overwrite").save(target_path)
        logger.info(" [+] Đã khởi tạo (Overwrite) fact_sales lần đầu")

    spark.stop()