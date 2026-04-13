from pyspark.sql.functions import col, md5
from common_utils import get_spark_session, apply_scd2, SILVER_PATH, GOLD_PATH, logger

if __name__ == "__main__":
    spark = get_spark_session("Build_Dim_Customer_SCD2")
    df_customers = spark.read.format("delta").load(SILVER_PATH + "customers/")
    dim_customer_prep = df_customers.withColumn("city_key", md5(col("customer_zip_code_prefix"))).select("customer_id", "customer_unique_id", "city_key")
    apply_scd2(spark, dim_customer_prep, GOLD_PATH + "dim_customer/", "dim_customer", "customer_id", "customer_key", ["city_key"])
    spark.stop()