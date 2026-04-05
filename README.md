# Hệ Thống Data Lakehouse cho Dữ Liệu Thương Mại Điện Tử Olist

## Giới Thiệu

Dự án này triển khai một hệ thống Data Lakehouse hoàn chỉnh theo kiến trúc Medallion (Bronze - Silver - Gold) để xử lý và phân tích dữ liệu thương mại điện tử từ nền tảng Olist (Brazil). Hệ thống hỗ trợ hai mô hình xử lý dữ liệu: Batch (Incremental) và Real-time CDC (Change Data Capture), đảm bảo tính nhất quán và cập nhật dữ liệu theo thời gian thực.

## Công Nghệ Sử Dụng

| Thành Phần                | Công Nghệ               |
| ------------------------- | ----------------------- |
| Cơ sở dữ liệu nguồn       | MySQL 8.0               |
| Change Data Capture (CDC) | Debezium + Apache Kafka |
| Lưu trữ dữ liệu           | MinIO (S3-compatible)   |
| Xử lý dữ liệu             | Apache Spark (PySpark)  |
| Định dạng bảng mở         | Delta Lake              |
| Điều phối quy trình       | Apache Airflow          |

## Yêu Cầu Hệ Thống

Để triển khai hệ thống, máy tính của bạn phải thỏa mãn các yêu cầu sau:

- **Docker**: Phiên bản 20.10 trở lên
- **Docker Compose**: Phiên bản 1.29 trở lên
- **Bộ nhớ RAM**: Tối thiểu 8GB (khuyến nghị 12GB+ để chạy Spark ổn định)
- **Dung lượng đĩa**: Tối thiểu 20GB không gian trống

## Hướng Dẫn Triển Khai

### Bước 1: Khởi Động Hạ Tầng

1. Mở terminal tại thư mục gốc của dự án
2. Chạy lệnh để khởi động tất cả các dịch vụ:

```bash
docker-compose up -d
```

3. Chờ 2-3 phút để tất cả các dịch vụ (MySQL, Kafka, Spark, MinIO, Airflow) hoàn toàn khởi động
4. Kiểm tra trạng thái các container:

```bash
docker-compose ps
```

### Bước 2: Kích Hoạt Luồng CDC (Change Data Capture)

Debezium được sử dụng để giám sát các thay đổi dữ liệu (Insert, Update, Delete) theo thời gian thực trên các bảng `orders` và `order_items` từ MySQL.

Đăng ký Connector với Kafka Connect bằng lệnh sau:

```bash
curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  localhost:8083/connectors/ \
  -d '{
    "name": "olist-connector-v2",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "tasks.max": "1",
      "database.hostname": "mysql",
      "database.port": "3306",
      "database.user": "root",
      "database.password": "admin",
      "database.server.id": "184054",
      "topic.prefix": "cdc_v2",
      "database.include.list": "olist_db",
      "table.include.list": "olist_db.orders,olist_db.order_items",
      "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
      "schema.history.internal.kafka.topic": "schema-changes.olist_db"
    }
  }'
```

**Lưu ý**: Để xóa hoặc cấu hình lại luồng CDC, sử dụng lệnh:

```bash
curl -X DELETE localhost:8083/connectors/olist-connector-v2
```

### Bước 3: Khởi Chạy Luồng Streaming (Kafka → Bronze Layer)

Luồng này sử dụng Spark Structured Streaming để tiếp nhận dữ liệu CDC từ Kafka theo thời gian thực và lưu trữ dưới định dạng Parquet trên MinIO.

Gửi Spark Job đến master node:

```bash
docker exec -it spark-master bash -c "spark-submit \
  --master spark://spark-master:7077 \
  /opt/airflow/etl/bronze/kafka_to_bronze.py"
```

### Bước 4: Xử Lý Dữ Liệu Batch và Nâng Cấp lên Silver Layer

1. Truy cập giao diện Apache Airflow tại: `http://localhost:8080`
   - Tài khoản mặc định: `admin_airflow`
   - Mật khẩu mặc định: `MatKhauWeb`

2. Kích hoạt các DAG theo thứ tự sau:

   **mysql_to_bronze_dag**: Trích xuất incremental dữ liệu từ các bảng còn lại (customers, products, v.v) từ MySQL và đẩy vào Bronze Layer

   **silver_spark_processor**: Xử lý và nâng cấp dữ liệu từ Bronze lên Silver Layer, bao gồm:
   - Làm sạch dữ liệu (Data Quality/Semantic Cleaning)
   - Kiểm soát chất lượng dữ liệu
   - Khử trùng lặp bản ghi (Deduplication)
   - Thực hiện UPSERT vào Delta Lake
   - Xử lý Soft Delete cho các bản ghi bị xóa ở MySQL (Hard Delete → Soft Delete)

## Kịch Bản Kiểm Thử và Minh Họa

Để minh chứng khả năng của hệ thống CDC và Delta Lake, bạn có thể thực hiện kịch bản Load Test như sau:

1. **Sinh dữ liệu giả lập**: Vào Airflow UI và kích hoạt DAG `00_Demo_Data_Generator`
   - DAG này sẽ thực thi hàng trăm lệnh Insert, Update, Delete vào MySQL

2. **Quan sát xử lý CDC**: Kiểm tra logs của Debezium và luồng Streaming (Bước 3)

3. **Xử lý dữ liệu**: Kích hoạt lại DAG `silver_spark_processor`

4. **Xác minh kết quả**: Sử dụng DBeaver hoặc DataGrip để kiểm tra bảng Delta ở Silver Layer:
   - Khách hàng đã cập nhật địa chỉ không bị nhân đôi
   - Các đơn hàng bị xóa khỏi MySQL vẫn được bảo toàn với flag `is_active = False` (Soft Delete)

## Cấu Trúc Thư Mục

```
├── airflow/                          # Định nghĩa quy trình công việc
│   ├── config/                       # Cấu hình Airflow
│   ├── dags/                         # Các DAG chính
│   ├── logs/                         # Logs từ các DAG runs
│   └── plugins/                      # Các plugins tùy chỉnh
├── dataset/                          # Dữ liệu Olist ban đầu (CSV)
├── docker_image/                     # Dockerfile cho các dịch vụ
├── etl/                              # Mã xử lý dữ liệu
│   ├── bronze/                       # Bronze Layer ETL
│   ├── silver/                       # Silver Layer ETL
│   ├── jars/                         # JAR files cho Spark
│   └── utils/                        # Các utilities chung
├── load_dataset_into_mysql/          # Script khởi tạo dữ liệu
└── docker-compose.yaml               # Cấu hình Docker Compose
```

## Khắc Phục Sự Cố

### Dịch vụ không khởi động được

- Kiểm tra dung lượng bộ nhớ: `docker stats`
- Xem logs dịch vụ: `docker-compose logs <service-name>`

### Lỗi kết nối đến MySQL

- Xác nhận MySQL đã hoàn toàn khởi động: `docker-compose logs mysql`
- Kiểm tra thông tin đăng nhập (user: root, password: admin)

### Lỗi Spark Job

- Xem logs chi tiết: `docker exec spark-master cat /var/log/spark/`
- Kiểm tra dung lượng bộ nhớ cấp cho Spark
