import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, desc, lit, from_json, when, year
from delta.tables import DeltaTable
from pyspark.sql.functions import trim, lower, upper, to_timestamp, lpad

logger = logging.getLogger(__name__)

def clean_table_data(df, table_name):
    """
    Clean data cho lớp Silver
    """

    cleaned_df = df

    # trim tất cả string (data lớp bronze thường khá bẩn)
    for column_name, dtype in cleaned_df.dtypes:
        if dtype == "string":
            cleaned_df = cleaned_df.withColumn(column_name, trim(col(column_name)))


    if table_name == "customers":
        # nomolize text cho dễ group/join
        cleaned_df = cleaned_df.withColumn("customer_city", lower(col("customer_city"))) \
            .withColumn("customer_state", upper(col("customer_state"))) \
            .withColumn("customer_zip_code_prefix", lpad(col("customer_zip_code_prefix"), 5, '0')) \
            .fillna({"customer_city": "unknown", "customer_state": "UN"})

    elif table_name == "sellers":
        # giống với customers
        cleaned_df = cleaned_df.withColumn("seller_city", lower(col("seller_city"))) \
            .withColumn("seller_state", upper(col("seller_state"))) \
            .withColumn("seller_zip_code_prefix", lpad(col("seller_zip_code_prefix"), 5, '0')) \
            .fillna({"seller_city": "unknown", "seller_state": "UN"})

    elif table_name == "orders":
        cleaned_df = cleaned_df.withColumn("order_status", lower(col("order_status")))

        # convert epoch -> timestamp
        cdc_time_columns = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
        for t_col in cdc_time_columns:
            if t_col in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(t_col, (col(t_col) / 1000).cast("timestamp"))

    elif table_name == "order_items":
        # cast về float cho chắc
        cleaned_df = cleaned_df.withColumn("price", col("price").cast("float")) \
            .withColumn("freight_value", col("freight_value").cast("float")) \
            .fillna({"price": 0.0, "freight_value": 0.0})

        if "shipping_limit_date" in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn("shipping_limit_date",
                                               (col("shipping_limit_date") / 1000).cast("timestamp"))

    elif table_name == "order_payments":
        # clean payment
        cleaned_df = cleaned_df.withColumn("payment_value", col("payment_value").cast("float")) \
            .withColumn("payment_installments", col("payment_installments").cast("integer")) \
            .withColumn("payment_type", lower(col("payment_type"))) \
            .fillna({"payment_value": 0.0, "payment_installments": 1})

    elif table_name == "order_reviews":
        # filter date lỗi (data thực tế thường bị)
        cleaned_df = cleaned_df.withColumn("review_score", col("review_score").cast("integer")) \
            .withColumn("review_creation_date", to_timestamp(col("review_creation_date"))) \
            .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))

        for date_col in ["review_creation_date", "review_answer_timestamp"]:
            cleaned_df = cleaned_df.withColumn(
                date_col,
                when((year(col(date_col)) < 2000) | (year(col(date_col)) > 2050), lit(None).cast("timestamp"))
                .otherwise(col(date_col))
            )

    elif table_name == "products":
        # ép kiểu mấy cột numeric
        cleaned_df = cleaned_df.withColumn("product_name_lenght", col("product_name_lenght").cast("float")) \
            .withColumn("product_description_lenght", col("product_description_lenght").cast("float")) \
            .withColumn("product_photos_qty", col("product_photos_qty").cast("integer")) \
            .withColumn("product_weight_g", col("product_weight_g").cast("float")) \
            .withColumn("product_length_cm", col("product_length_cm").cast("float")) \
            .withColumn("product_height_cm", col("product_height_cm").cast("float")) \
            .withColumn("product_width_cm", col("product_width_cm").cast("float"))

    elif table_name == "geolocation":
        # zip code phải đủ năm số 
        cleaned_df = cleaned_df.withColumn("geolocation_zip_code_prefix",
                                           lpad(col("geolocation_zip_code_prefix"), 5, '0')) \
            .withColumn("geolocation_lat", col("geolocation_lat").cast("float")) \
            .withColumn("geolocation_lng", col("geolocation_lng").cast("float")) \
            .withColumn("geolocation_city", lower(col("geolocation_city"))) \
            .withColumn("geolocation_state", upper(col("geolocation_state")))

    return cleaned_df

def process_silver_layer(
        spark: SparkSession,
        table_name: str,
        merge_key: str,
        mysql_config: dict,
        watermark_col: str = "last_modified_date",
        is_cdc: bool = False,       
        json_schema: str = None,        
        is_full_load: bool = False
):
    """
    Setup batch
    """

    merge_keys = [k.strip() for k in merge_key.split(",")]
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    bronze_path = f"s3a://lakehouse/bronze/olist/{table_name}/"
    silver_table_name = f"silver.clean_{table_name}"
    silver_table_path = f"s3a://lakehouse/silver/clean_{table_name}/"
    checkpoint_path = f"s3a://lakehouse/checkpoints/silver/{table_name}/"

    logger.info(f" Bắt đầu xử lý lớp Silver cho bảng: {table_name} | Chế độ CDC: {is_cdc}")

    # full load
    if is_full_load:
        logger.info(" Chế độ BATCH: Đọc toàn bộ file data.parquet bị ghi đè")

        df_bronze = spark.read.format("parquet").load(bronze_path)

        # duplicate theo key
        window_spec = Window.partitionBy(*merge_keys).orderBy(desc(watermark_col))
        final_df = df_bronze.withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1).drop("rn") \
            .withColumn("is_active", lit(True))

        final_df = clean_table_data(final_df, table_name)

        if DeltaTable.isDeltaTable(spark, silver_table_path):
            delta_table = DeltaTable.forPath(spark, silver_table_path)

            logger.info(" Tiến hành Upsert & Tự động Soft Delete")
            (delta_table.alias("target")
             .merge(final_df.alias("source"), merge_condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .whenNotMatchedBySourceUpdate(set={"is_active": lit(False)})
             .execute())
        else:
            logger.info("Khởi tạo bảng Silver Full Load lần đầu")
            spark.sql("CREATE DATABASE IF NOT EXISTS silver")
            final_df.write.format("delta").mode("overwrite").option("path", silver_table_path).saveAsTable(
                f"silver.clean_{table_name}")

        logger.info(f" Xong Full Load bảng {table_name}. (Không cần chạy Anti-Join Phase 2)")

        return

    # streaming
    def process_micro_batch(micro_batch_df, batch_id):
        if micro_batch_df.isEmpty():
            return

        if is_cdc:
            # parse json từ debezium
            parsed_df = micro_batch_df.withColumn("parsed_data", from_json(col("raw_record"), json_schema)) \
                .select("parsed_data.*", "op", "ts_ms")

            # lấy record mới nhất
            window_spec = Window.partitionBy(*merge_keys).orderBy(desc("ts_ms"))
            deduped_df = parsed_df.withColumn("rn", row_number().over(window_spec)) \
                .filter(col("rn") == 1).drop("rn")

            # nếu op = d thì coi như delete
            final_df = deduped_df.withColumn("is_active", when(col("op") == 'd', lit(False)).otherwise(lit(True))) \
                .drop("op", "ts_ms")
        else:
            # normal streaming
            window_spec = Window.partitionBy(*merge_keys).orderBy(desc(watermark_col))
            final_df = micro_batch_df.withColumn("rn", row_number().over(window_spec)) \
                .filter(col("rn") == 1).drop("rn") \
                .withColumn("is_active", lit(True))

        final_df = clean_table_data(final_df, table_name)

        # upset vào delta
        if DeltaTable.isDeltaTable(spark, silver_table_path):
            delta_table = DeltaTable.forPath(spark, silver_table_path)


            (delta_table.alias("target")
             .merge(final_df.alias("source"), merge_condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())

            metrics = delta_table.history(1).select("operationMetrics").collect()[0][0]
            num_updated = metrics.get("numTargetRowsUpdated", "0")
            num_inserted = metrics.get("numTargetRowsInserted", "0")

            logger.info(f"   [Batch {batch_id}] Kết quả: Update {num_updated} dòng | Insert {num_inserted} dòng mới.")
        else:
            spark.sql("CREATE DATABASE IF NOT EXISTS silver")
            final_df.write.format("delta").mode("overwrite") \
                .option("path", silver_table_path) \
                .saveAsTable(f"silver.clean_{table_name}")

            dt = DeltaTable.forPath(spark, silver_table_path)
            metrics = dt.history(1).select("operationMetrics").collect()[0][0]
            num_inserted = metrics.get("numOutputRows", "0")

            logger.info(f"   [Batch {batch_id}] Khởi tạo thành công: Insert {num_inserted} dòng đầu tiên.")

    logger.info(" Bắt đầu quét Data Nóng từ Bronze")
    streaming_query = (spark.readStream
                       .format("parquet")
                       .load(bronze_path)
                       .writeStream
                       .foreachBatch(process_micro_batch)
                       .option("checkpointLocation", checkpoint_path)
                       .trigger(availableNow=True)
                       .start())

    streaming_query.awaitTermination()
    logger.info(" Hoàn thành quét Data Nóng cập nhật vào Silver.")

    # hard delete
    if is_cdc:
        logger.info(" Bảng chạy chế độ CDC -> Bỏ qua bước Anti-Join.")
    elif DeltaTable.isDeltaTable(spark, silver_table_path):
        logger.info(" Bắt đầu dò tìm 'bóng ma' Hard Delete")

        mysql_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
        source_ids_df = (spark.read.format("jdbc")
                         .option("url", mysql_url)
                         .option("driver", "com.mysql.cj.jdbc.Driver")
                         .option("dbtable", f"(SELECT {merge_key} FROM {table_name}) AS tmp")
                         .option("user", mysql_config['user'])
                         .option("password", mysql_config['password'])
                         .option("fetchsize", "10000")
                         .load())

        if "zip_code_prefix" in merge_key:
            source_ids_df = source_ids_df.withColumn(merge_key, lpad(col(merge_key).cast("string"), 5, '0'))

        delta_table = DeltaTable.forPath(spark, silver_table_path)
        silver_active_df = delta_table.toDF().filter("is_active == True")

        # anti join -> record không còn ở source
        deleted_records = silver_active_df.join(source_ids_df, on=merge_keys, how="left_anti")
        deleted_count = deleted_records.count()

        if deleted_count > 0:
            logger.info(f" Phát hiện {deleted_count} bản ghi bị Hard Delete. Tiến hành Soft Delete trên Silver")
            update_condition = " AND ".join([f"t.{k} = d.{k}" for k in merge_keys])
            (delta_table.alias("t")
             .merge(deleted_records.alias("d"), update_condition)
             .whenMatchedUpdate(set={"is_active": lit(False)})
             .execute())
            logger.info(f" THÀNH CÔNG: Đã chuyển trạng thái is_active = False cho {deleted_count} dòng.")
        else:
            logger.info(" Ko phát hiện bản ghi Hard Delete nào.")

    logger.info(f" Xử lý lớp Silver xong cho bảng: {table_name}\n" + "-" * 40)
