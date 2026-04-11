from pyspark.sql.functions import col, md5, date_format, when, datediff, expr
from common_utils import get_spark_session, SILVER_PATH, GOLD_PATH, logger
from delta.tables import DeltaTable

if __name__ == "__main__":
    spark = get_spark_session("Build_Fact_Shipment")
    df_orders = spark.read.format("delta").load(SILVER_PATH + "orders/")
    df_items = spark.read.format("delta").load(SILVER_PATH + "order_items/")
    gold_dim_cust = spark.read.format("delta").load(GOLD_PATH + "dim_customer/")
    gold_dim_seller = spark.read.format("delta").load(GOLD_PATH + "dim_seller/")

    fact_shipment = (df_orders.alias("o").join(df_items.select("order_id", "seller_id", "shipping_limit_date").dropDuplicates(["order_id"]).alias("i"), "order_id", "inner") \
        .join(gold_dim_cust.alias("c"), expr("o.customer_id = c.customer_id AND o.order_purchase_timestamp >= c.effective_date AND (o.order_purchase_timestamp < c.expiry_date OR c.expiry_date IS NULL)"), "left") \
        .join(gold_dim_seller.alias("s"), expr("i.seller_id = s.seller_id AND o.order_purchase_timestamp >= s.effective_date AND (o.order_purchase_timestamp < s.expiry_date OR s.expiry_date IS NULL)"), "left") \
        .withColumn("shipment_key", md5(col("o.order_id")))
        .withColumn("shipping_limit_date_key", date_format(col("i.shipping_limit_date"), "yyyyMMdd").cast("int"))
        .withColumn("delivered_carrier_date_key", date_format(col("o.order_delivered_carrier_date"), "yyyyMMdd").cast("int"))
        .withColumn("delivered_customer_date_key", date_format(col("o.order_delivered_customer_date"), "yyyyMMdd").cast("int"))
        .withColumn("estimated_delivery_date_key",date_format(col("o.order_estimated_delivery_date"), "yyyyMMdd").cast("int"))
        .withColumn("delivery_delay_days", datediff(col("o.order_delivered_customer_date"), col("o.order_estimated_delivery_date")))
        .withColumn("carrier_to_customer_days", datediff(col("o.order_delivered_customer_date"), col("o.order_delivered_carrier_date")))
        .withColumn("is_late_delivery", when(col("delivery_delay_days") > 0, 1).otherwise(0)) \
        .select("shipment_key", "o.order_id", "c.customer_key", "s.seller_key",
                col("shipping_limit_date_key").alias("shipping_limit_date"),
                col("delivered_carrier_date_key").alias("delivered_carrier_date"),
                col("delivered_customer_date_key").alias("delivered_customer_date"),
                col("estimated_delivery_date_key").alias("estimated_delivery_date"),
                "delivery_delay_days", "carrier_to_customer_days", "is_late_delivery"))

    target_path = GOLD_PATH + "fact_shipment/"

    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        logger.info(" [*] Tiến hành MERGE (Incremental Load) bảng fact_shipment...")

        (delta_table.alias("t")
         .merge(fact_shipment.alias("s"), "t.shipment_key = s.shipment_key")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

        logger.info(" [+] Đã MERGE xong fact_shipment")
    else:
        fact_shipment.write.format("delta").mode("overwrite").save(target_path)
        logger.info(" [+] Đã khởi tạo (Overwrite) fact_shipment lần đầu")

    spark.stop()