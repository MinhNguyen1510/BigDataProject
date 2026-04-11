from pyspark.sql.functions import col, md5
from common_utils import get_spark_session, apply_scd2, SILVER_PATH, GOLD_PATH, logger

if __name__ == "__main__":
    spark = get_spark_session("Build_Dim_Product_SCD2")
    df_products = spark.read.format("delta").load(SILVER_PATH + "products/")
    dim_product_prep = df_products.withColumn("category_key", md5(col("product_category_name"))).select("product_id", "category_key", col("product_weight_g").alias("weight"), col("product_length_cm").alias("length"), col("product_height_cm").alias("height"), col("product_width_cm").alias("width"))
    apply_scd2(spark, dim_product_prep, GOLD_PATH + "dim_product/", "product_id", "product_key", ["category_key", "weight", "length", "height", "width"])
    spark.stop()