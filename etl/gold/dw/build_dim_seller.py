from pyspark.sql.functions import col, md5
from common_utils import get_spark_session, apply_scd2, SILVER_PATH, GOLD_PATH, logger

if __name__ == "__main__":
    spark = get_spark_session("Build_Dim_Seller_SCD2")
    df_sellers = spark.read.format("delta").load(SILVER_PATH + "sellers/")
    dim_seller_prep = df_sellers.withColumn("city_key", md5(col("seller_zip_code_prefix"))).select("seller_id", "city_key")
    apply_scd2(spark, dim_seller_prep, GOLD_PATH + "dim_seller/", "seller_id", "seller_key", ["city_key"])
    spark.stop()