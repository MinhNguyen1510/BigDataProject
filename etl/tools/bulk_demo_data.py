import pymysql
import uuid
import random
from datetime import datetime

DB_CONFIG = {
    'host': 'mysql',
    'port': 3306,
    'user': 'root',
    'password': 'admin',
    'database': 'olist-db',
    'autocommit': True
}

NUM_RECORDS = 50


def get_connection():
    return pymysql.connect(**DB_CONFIG)


def run_bulk_demo():
    conn = get_connection()
    cursor = conn.cursor()

    print(f" BẮT ĐẦU DỘI BOM {NUM_RECORDS} DÒNG CHO MỖI THAO TÁC...\n")

    # BẢNG CUSTOMERS (INCREMENTAL LUỒNG BATCH)
    print("Đang xử lý bảng CUSTOMERS...")

    new_customers = [(uuid.uuid4().hex, uuid.uuid4().hex, str(random.randint(10000, 99999)), 'ho chi minh', 'SG') for _
                     in range(NUM_RECORDS)]
    sql_insert_cust = "INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(sql_insert_cust, new_customers)
    print(f"  [+] Đã Insert {NUM_RECORDS} khách hàng mới.")

    cursor.execute(f"SELECT customer_id FROM customers LIMIT {NUM_RECORDS}")
    cust_to_update = [(c[0],) for c in cursor.fetchall()]
    sql_update_cust = "UPDATE customers SET customer_city = 'da nang', customer_state = 'DN' WHERE customer_id = %s"
    cursor.executemany(sql_update_cust, cust_to_update)
    print(f"  [*] Đã Update {len(cust_to_update)} khách hàng cũ chuyển nhà ra Đà Nẵng.")

    # BẢNG ORDERS (LUỒNG CDC - DEBEZIUM)
    print("\n. Đang xử lý bảng ORDERS (CDC)...")
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # BATCH INSERT (Tạo đơn hàng từ chính những khách hàng vừa tạo ở trên để chuẩn khóa ngoại)
    new_orders = [(uuid.uuid4().hex, cust[0], 'created', current_time) for cust in new_customers]
    sql_insert_order = "INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql_insert_order, new_orders)
    print(f"  [+] Đã Insert {NUM_RECORDS} đơn hàng mới.")

    # BATCH UPDATE (Đổi trạng thái đơn cũ thành 'delivered')
    cursor.execute(f"SELECT order_id FROM orders WHERE order_status != 'delivered' LIMIT {NUM_RECORDS}")
    orders_to_update = [(current_time, o[0]) for o in cursor.fetchall()]
    sql_update_order = "UPDATE orders SET order_status = 'delivered', order_delivered_customer_date = %s WHERE order_id = %s"
    cursor.executemany(sql_update_order, orders_to_update)
    print(f"  [*] Đã Update {len(orders_to_update)} đơn hàng thành 'delivered'.")

    # BATCH HARD DELETE (Xóa vĩnh viễn đơn hàng)
    cursor.execute(f"SELECT order_id FROM orders LIMIT {NUM_RECORDS} OFFSET 500")  # Né mấy đơn vừa update ra
    orders_to_delete = [(o[0],) for o in cursor.fetchall()]
    sql_delete_order = "DELETE FROM orders WHERE order_id = %s"
    cursor.executemany(sql_delete_order, orders_to_delete)
    print(f"  [-] Đã HARD DELETE {len(orders_to_delete)} đơn hàng.")

    # ---------------------------------------------------------
    # BẢNG ORDER_ITEMS (LUỒNG CDC - DEBEZIUM)
    # ---------------------------------------------------------
    print("\n3. Đang xử lý bảng ORDER_ITEMS (CDC)...")

    # BATCH UPDATE (Tăng giá sản phẩm lên 10%)
    cursor.execute(f"SELECT order_id, order_item_id FROM order_items LIMIT {NUM_RECORDS}")
    items_to_update = [(item[0], item[1]) for item in cursor.fetchall()]
    sql_update_item = "UPDATE order_items SET price = price * 1.10 WHERE order_id = %s AND order_item_id = %s"
    cursor.executemany(sql_update_item, items_to_update)
    print(f"  [*] Đã Update tăng giá 10% cho {len(items_to_update)} món hàng.")

    # BATCH HARD DELETE (Khách hủy món hàng)
    cursor.execute(f"SELECT order_id, order_item_id FROM order_items LIMIT {NUM_RECORDS} OFFSET 200")
    items_to_delete = [(item[0], item[1]) for item in cursor.fetchall()]
    sql_delete_item = "DELETE FROM order_items WHERE order_id = %s AND order_item_id = %s"
    cursor.executemany(sql_delete_item, items_to_delete)
    print(f"  [-] Đã HARD DELETE {len(items_to_delete)} món hàng.")

    conn.close()
    print("\n XONG! Dữ liệu đã văng mù mịt vào MySQL. Khởi động Airflow ngay!")


if __name__ == "__main__":
    run_bulk_demo()