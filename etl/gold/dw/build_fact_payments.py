from pyspark.sql.functions import col, md5, date_format, count, sum as _sum, first, max as _max, round
from common_utils import get_spark_session, SILVER_PATH, GOLD_PATH, logger
from delta.tables import DeltaTable

if __name__ == "__main__":
    spark = get_spark_session("Build_Fact_Payments")
    df_orders = spark.read.format("delta").load(SILVER_PATH + "orders/")
    df_payments = spark.read.format("delta").load(SILVER_PATH + "order_payments/")

    fact_payments = df_payments.groupBy("order_id").agg(count("payment_sequential").alias("Num_payment_methods"), round(_sum("payment_value"), 2).alias("Total_payment_value"), _max("payment_installments").alias("payment_installments"), first("payment_type").alias("primary_payment_type")) \
        .join(df_orders, "order_id", "left").withColumn("payment_type_key", md5(col("primary_payment_type"))).withColumn("time_key", date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int")).select("order_id", "payment_type_key", "Num_payment_methods", "time_key", "Total_payment_value", "payment_installments")

    target_path = GOLD_PATH + "fact_payments/"
    spark.sql("CREATE DATABASE IF NOT EXISTS dw_db LOCATION 's3a://lakehouse/gold/dw/'")

    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forName(spark, "dw_db.fact_payments")
        logger.info(" [*] Tiến hành MERGE (Incremental Load) bảng fact_payments...")

        (delta_table.alias("t")
         .merge(fact_payments.alias("s"), "t.order_id = s.order_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

        logger.info(" [+] Đã MERGE xong fact_payments")
    else:
        fact_payments.write.format("delta").mode("overwrite") \
            .option("path", target_path) \
            .saveAsTable("dw_db.fact_payments")
        logger.info(" [+] Khởi tạo dw_db.fact_payments")

    spark.stop()