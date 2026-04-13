# 🏗️ Hệ Thống Data Lakehouse cho Dữ Liệu Thương Mại Điện Tử Olist

## Giới Thiệu

Dự án này triển khai một hệ thống **Data Lakehouse** hoàn chỉnh theo kiến trúc **Medallion (Bronze - Silver - Gold)** để xử lý và phân tích dữ liệu thương mại điện tử từ nền tảng **Olist** (Brazil).

Hệ thống xử lý dữ liệu qua các lớp:

- **Bronze Layer**: Dữ liệu thô từ MySQL (batch) + dữ liệu thực tế từ CDC Kafka
- **Silver Layer**: Dữ liệu sạch, đã xử lý (loại bỏ trùng lặp, upsert, soft delete) sử dụng Spark + Delta Lake
- **Gold Layer**: Data Warehouse và Data Marts để phân tích kinh doanh

## Công Nghệ Sử Dụng

| Thành Phần                  | Công Nghệ                    |
| --------------------------- | ---------------------------- |
| Cơ sở dữ liệu nguồn         | MySQL 8.0                    |
| Change Data Capture (CDC)   | Debezium + Kafka             |
| Lưu trữ dữ liệu (Data Lake) | MinIO (S3-compatible)        |
| Xử lý dữ liệu               | Apache Spark 3.3.2 (PySpark) |
| Định dạng bảng mở           | Delta Lake 2.3.0             |
| Điều phối quy trình         | Apache Airflow               |
| Số liệu hóa                 | Apache Superset              |
| ML Tracking                 | MLflow                       |
| Metadata Store              | Hive Metastore               |

## Yêu Cầu Hệ Thống

- **Docker**: Phiên bản 20.10+
- **Docker Compose**: Phiên bản 1.29+
- **RAM**: Tối thiểu 16GB (khuyến nghị 24GB+ để chạy Spark ổn định)
- **Dung lượng đĩa**: Tối thiểu 40GB không gian trống
- **Hệ điều hành**: Linux, macOS hoặc Windows (với WSL2)

## Kiến Trúc Hệ Thống

```
┌─────────────────────────────────────────────────────────────────┐
│  MySQL (Olist DB) + Initial Dataset                             │
├─────────────────┬──────────────────┬──────────────────┬──────────┤
│ Batch Extract   │  CDC (Kafka)     │                  │          │
│ (via Airflow)   │  → Debezium      │                  │          │
└────────┬────────┴────────┬─────────┴──────────────────┴──────────┘
         │                 │
         │         ┌───────▼────────┐
         │         │  Kafka Broker  │
         │         └───────┬────────┘
         │                 │
    ┌────▼────────────────▼────────────┐
    │   BRONZE LAYER (MinIO)           │
    │   • Parquet format (batch data)  │
    │   • Real-time CDC data           │
    └────┬─────────────────────────────┘
         │
    ┌────▼────────────────────────────────┐
    │   SILVER LAYER (MinIO)              │
    │   • Delta Lake format               │
    │   • Merged & deduped data           │
    │   • Soft deletes & upserts handled  │
    └────┬─────────────────────────────────┘
         │
    ┌────▼────────────────────────────────┐
    │   GOLD LAYER (MinIO)                │
    │   • Data Warehouse (fact/dimension) │
    │   • Data Marts (analysis-ready)     │
    │   • ML Features                     │
    └─────────────────────────────────────┘
```

## Hướng Dẫn Chi Tiết Triển Khai

### **BƯỚC 1: Chuẩn Bị - mở Comment các Service**

Trước khi chạy, bạn cần ** mở comment hết các service ** trong file `docker-compose.yaml`.

Mở file `docker-compose.yaml` và comment các services sau (thêm `# ` vào đầu các dòng hoặc sử dụng phím Ctrl+K Ctrl+C):

**Giữ lại toàn bộ dịch vụ để build docker lần đầu tiên:**

- debezium-connect
- kafka
- zookeeper
- spark-master, spark-worker-1, spark-worker-2, hive-metastore
- superset
- mlflow
- postgres
- mysql
- minio
- airflow-postgres
- airflow-init
- airflow-webserver
- airflow-scheduler
- airflow-worker
- init-olist-data

### **BƯỚC 2: Khởi Động Hạ Tầng**

Chạy lệnh sau để build và khởi động tất cả các container:

```bash
docker compose up -d --build
```

Quá trình này mất khoảng 15-20 phút tùy vào tốc độ mạng.

### **BƯỚC 3: Kiểm Tra Dữ Liệu MySQL**

Chạy lệnh sau để xem logs của container `init-olist-data` và đảm bảo dữ liệu đã được import vào MySQL:

```bash
docker compose logs -f init-olist-data
```

Chờ cho đến khi thấy thông báo import hoàn tất. Sau đó nhấn `Ctrl+C` để thoát.

### **BƯỚC 4: Chạy Bronze DAG (Extract từ MySQL)**

1. Mở trình duyệt và truy cập **Airflow Webserver**: `http://localhost:8080`
2. Đăng nhập với thông tin:
   - Username: `admin_airflow`
   - Password: `MatKhauWeb`

3. Tìm và nhấn nút **Play** để chạy DAG tên `bronze_mysql_to_minio`

4. Chờ cho đến khi tất cả tasks trong DAG có trạng thái **SUCCESS** (xanh lá cây)

### **BƯỚC 5: Kiểm Tra Bronze Layer trên MinIO**

Sau khi Bronze DAG chạy xong, kiểm tra dữ liệu đã được đẩy lên MinIO:

1. Mở trình duyệt truy cập: `http://localhost:9001`
2. Đăng nhập với:
   - Username: `minio`
   - Password: `minio123`

3. Tìm bucket `lakehouse` và xem các folder:
   - `bronze/customers/`
   - `bronze/sellers/`
   - `bronze/products/`
   - `bronze/order_payments/`
   - `bronze/order_reviews/`
   - `bronze/product_category_name_translation/`
   - `bronze/geolocation/`

Đảm bảo các file Parquet đã được tạo.

### **BƯỚC 6: Kích Hoạt CDC (Change Data Capture) - TÙY CHỌN**

Nếu bạn muốn bật streaming real-time từ Kafka:

**6.1 Các service Kafka/Debezium**

- debezium-connect
- kafka
- zookeeper

**6.2 Đăng ký Kafka Connector (Debezium)**

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "olist-connector-v2",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "admin",
    "database.server.id": "99999",
    "topic.prefix": "cdc_v2",
    "database.include.list": "olist_db",
    "table.include.list": "olist_db.orders,olist_db.order_items",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-history-v2",
    "decimal.handling.mode": "double"
  }
}'
```

> **Lưu ý Windows**: Nếu dùng Windows PowerShell, bạn có thể gặp lỗi. Hãy sử dụng **Git Bash** để chạy lệnh curl này.

**6.3 Khởi Chạy Spark Streaming Job**

Sau khi connector đã được đăng ký, chạy lệnh sau để bắt đầu tiếp nhận dữ liệu CDC từ Kafka vào Bronze Layer:

```bash
docker exec -it spark-master bash -c "spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,io.delta:delta-core_2.12:2.3.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/airflow/etl/bronze/kafka_to_bronze.py"
```

Chờ cho đến khi thấy in ra logs cho thấy dữ liệu đang được xả vào MinIO. Lúc này, quay lại MinIO web và kiểm tra xem các folder mới đã xuất hiện:

- `bronze/orders/`
- `bronze/order_items/`

**6.4 Tắt Dịch Vụ Streaming (không bắt buộc)**

Nếu bạn muốn tiết kiệm tài nguyên sau khi kiểm tra CDC, bạn có thể tắt các dịch vụ streaming:

```bash
docker stop kafka zookeeper debezium-connect
docker rm kafka zookeeper debezium-connect
```

### **BƯỚC 7: Chạy Silver Layer DAG**

Silver Layer xử lý dữ liệu từ Bronze, loại bỏ trùng lặp, merge / upsert, và xử lý soft delete.

1. Trở lại Airflow Webserver: `http://localhost:8080`
2. Tìm DAG tên `silver_spark_processor`
3. Nhấn nút **Play** để chạy DAG
4. Chờ cho đến khi tất cả tasks có trạng thái **SUCCESS**

Dữ liệu Silver sẽ được lưu trữ ở MinIO bucket `lakehouse` dưới thư mục `silver/` dưới định dạng **Delta Lake**.

### **BƯỚC 8: Chạy Gold Layer DAG**

Gold Layer xây dựng Data Warehouse (fact/dimension tables) và Data Marts để phục vụ phân tích.

1. Trở lại Airflow Webserver: `http://localhost:8080`
2. Tìm DAG tên `Gold_Data_Warehouse`
3. Nhấn nút **Play** để chạy DAG
4. Chờ cho đến khi tất cả tasks có trạng thái **SUCCESS**

Sau khi hoàn tất, bạn có thể chạy thêm DAG `Gold_Mart_and_ML_Features` nếu muốn tạo Data Marts và ML Features.

---

## 📁 Cấu Trúc Dự Án

```
/root
├── airflow/                          # Cấu hình và DAGs của Airflow
│   ├── config/                       # Cấu hình Airflow
│   ├── dags/                         # DAG definitions
│   │   ├── bronze_dag.py             # Extract MySQL → Bronze
│   │   ├── silver_processor_dag.py   # Bronze → Silver (xử lý & merge)
│   │   ├── gold_data_warehouse.py    # Silver → Gold DW
│   │   ├── gold_mart_ml_dag.py       # Gold Marts & ML Features
│   │   └── demo_generator_dag.py     # Demo data generator
│   └── logs/                         # Logs của các DAG runs
│
├── etl/                              # Logic xử lý dữ liệu
│   ├── bronze/
│   │   ├── extract_mysql.py          # Extract từ MySQL
│   │   └── kafka_to_bronze.py        # Stream từ Kafka → Bronze
│   ├── silver/
│   │   ├── silver_processor.py       # Xử lý & merge data
│   │   └── main_silver.py            # Entry point Silver
│   ├── gold/
│   │   ├── dw/                       # Data Warehouse logic
│   │   └── mart/                     # Data Marts logic
│   ├── ml_features/                  # ML Feature engineering
│   └── utils/
│       ├── minio_client.py           # MinIO utilities
│       └── mysql_client.py           # MySQL utilities
│
├── dataset/                          # Raw CSV datasets
│   ├── olist_*.csv                   # Original Olist datasets
│   └── product_category_name_translation.csv
│
├── docker_image/                     # Dockerfiles
│   ├── airflow/
│   ├── spark/
│   ├── hive-metastore/
│   ├── mlflow/
│   └── superset/
│
├── load_dataset_into_mysql/          # SQL scripts để load data
│   ├── 01_create_tables.sql
│   └── load_data.py
│
├── docker-compose.yaml               # Orchestration container
├── .env                              # Environment variables
└── README.md                         # Tài liệu này
```

---

## Các Dịch Vụ & Port

| Dịch Vụ                     | URL                            | Thông Đăng Nhập                |
| --------------------------- | ------------------------------ | ------------------------------ |
| **Airflow Webserver**       | `http://localhost:8080`        | `admin_airflow` / `MatKhauWeb` |
| **MinIO Console**           | `http://localhost:9001`        | `minio` / `minio123`           |
| **MinIO API**               | `http://localhost:9000`        | -                              |
| **MySQL**                   | `localhost:3306`               | `root` / `admin`               |
| **Kafka**                   | `localhost:9092`               | -                              |
| **Spark Master UI**         | `http://localhost:7077`        | -                              |
| **Hive Metastore**          | `thrift://hive-metastore:9083` | -                              |
| **PostgreSQL (Airflow DB)** | `localhost:5432`               | `airflow` / `airflow`          |

---

## Một Số Lệnh Hữu Ích

### Kiểm tra trạng thái container:

```bash
docker compose ps
```

### Xem logs của một service:

```bash
docker compose logs -f <service_name>
```

### Truy cập Spark Master container:

```bash
docker exec -it spark-master bash
```

### Xóa Kafka Connector (nếu cần cấu hình lại):

```bash
curl -X DELETE localhost:8083/connectors/olist-connector-v2
```

### Dừng toàn bộ hệ thống:

```bash
docker compose down
```

### Xóa tất cả volumes (cân thận - xóa tất cả dữ liệu):

```bash
docker compose down -v
```

---

## ⚠️ Ghi Chú Quan Trọng

1. **Windows Users**: Khi chạy lệnh `curl`, vui lòng sử dụng **Git Bash** hoặc **WSL** thay vì PowerShell để tránh lỗi.

2. **RAM Requirements**: Nếu bạn chạy tất cả services cùng một lúc, cần ít nhất 16GB RAM. Để tiết kiệm, comment các services không cần thiết.

3. **Spark Job Streaming**: Khi chạy `kafka_to_bronze.py`, job này sẽ chạy liên tục (foreground). Để dừng, nhấn `Ctrl+C`.

4. **MinIO Data**: Toàn bộ dữ liệu được lưu trong volume `miniodata`. Nếu xóa volume này, bạn sẽ mất hết dữ liệu.

5. **Airflow DAGs**: Tất cả DAGs được tải từ folder `airflow/dags/`. Nếu bạn muốn thêm DAG mới, hãy tạo file Python trong folder này và restart Airflow scheduler.

---

## Nội Dung Chi Tiết Từng Lớp

### Bronze Layer

- Dữ liệu thô từ MySQL (extract batch hàng ngày qua Airflow)
- Dữ liệu real-time từ CDC trên bảng `orders` và `order_items`
- Format: **Parquet** (dễ nén, hiệu suất cao)
- Vị trí MinIO: `lakehouse/bronze/`

### Silver Layer

- Dữ liệu đã qua xử lý: deduplication, upsert, soft delete handling
- Sử dụng **UPSERT** pattern để merge dữ liệu mới với dữ liệu cũ
- Format: **Delta Lake** (ACID transactions, time travel)
- Vị trí MinIO: `lakehouse/silver/`

### Gold Layer

- **Data Warehouse**: Fact tables (orders, order_items) & Dimension tables (customers, products, sellers, etc.)
- **Data Marts**: Aggregate tables cho phân tích (sales by category, customer lifetime value, etc.)
- **ML Features**: Feature tables cho machine learning (product popularity, seller reputation, etc.)
- Format: **Delta Lake**
- Vị trí MinIO: `lakehouse/gold/`

---

## 📚 Tham Khảo Thêm

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Debezium Documentation](https://debezium.io/documentation/)

---

## Tác Giả

Dự án này được phát triển như một **đồ án môn học** để demonstate một hệ thống Data Lakehouse hoàn chỉnh theo best practices của ngành công nghiệp.

---

**Happy Data Engineering!**

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
