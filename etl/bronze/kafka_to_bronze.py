import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, current_timestamp, date_format

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def stream_kafka_to_bronze(spark: SparkSession, topic_name: str, table_name: str):
    """
    Hút dữ liệu CDC từ Kafka, bóc tách JSON và ghi vô lớp Bronze (MinIO)
    """

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    bronze_path = f"s3a://lakehouse/bronze/olist/{table_name}/"
    checkpoint_path = f"s3a://lakehouse/checkpoints/bronze/{table_name}_kafka/"

    logger.info(f" Khởi động luồng Streaming cho Topic: {topic_name} -> Bảng: {table_name}")

    # Định nghĩa Schema linh hoạt (Chỉ lấy những thành phần lõi của Debezium)
    # Vì chúng ta giữ data gốc, ta dùng kiểu STRING cho toàn bộ before/after để tránh lỗi ép kiểu
    debezium_schema = """
        payload STRUCT<
            before: STRING,
            after: STRING,
            op: STRING,
            source: STRUCT<ts_ms: LONG>
        >
    """

    # Kết nối ống hút vào Kafka
    raw_kafka_df = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafka_servers)
                    .option("subscribe", topic_name)
                    .option("startingOffsets", "earliest")  # Lấy từ đầu nếu có lỡ tắt máy
                    .option("failOnDataLoss", "false")
                    .load())

    # Bóc tách JSON (Giữ nguyên data gốc, không xào nấu)
    # - Cột value của Kafka là kiểu Binary, cần ép sang String
    # - Nếu là thao tác Xóa (op='d'), after sẽ bị null, ta phải lấy data từ before để biết ID nào bị xóa
    parsed_df = raw_kafka_df.select(
        from_json(col("value").cast("string"), debezium_schema).alias("data")
    ).select(
        expr("COALESCE(data.payload.after, data.payload.before)").alias("raw_record"),
        col("data.payload.op").alias("op"),
        col("data.payload.source.ts_ms").alias("ts_ms")
    )

    # Phân mảnh theo ngày (Logical Date) để chuẩn format lớp Bronze
    # Thêm cột năm, tháng, ngày dựa vào thời gian Spark xử lý để chia folder cho đẹp
    final_df = parsed_df.withColumn("processing_time", current_timestamp()) \
        .withColumn("year", date_format(col("processing_time"), "yyyy")) \
        .withColumn("month", date_format(col("processing_time"), "MM")) \
        .withColumn("day", date_format(col("processing_time"), "dd"))

    # Mở van xả xuống MinIO
    # Dùng trigger(processingTime="1 minute") để cứ 1 phút Spark đóng gói 1 file Parquet thả xuống MinIO
    logger.info(" Bắt đầu xả data xuống MinIO...")
    query = (final_df.writeStream
             .format("parquet")
             .option("path", bronze_path)
             .option("checkpointLocation", checkpoint_path)
             .partitionBy("year", "month", "day")
             .trigger(processingTime="1 minute")
             .start())

    return query


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("Kafka_To_Bronze_Streaming")
             .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
             .config("spark.hadoop.fs.s3a.access.key", "minio")
             .config("spark.hadoop.fs.s3a.secret.key", "minio123")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Kích hoạt 2 luồng chạy song song cho 2 bảng
    query_orders = stream_kafka_to_bronze(
        spark=spark,
        topic_name="cdc_v2.olist_db.orders",
        table_name="orders"
    )

    query_order_items = stream_kafka_to_bronze(
        spark=spark,
        topic_name="cdc_v2.olist_db.order_items",
        table_name="order_items"
    )

    spark.streams.awaitAnyTermination()