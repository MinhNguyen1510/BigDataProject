import pymysql
import uuid
import random
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

DB_CONFIG = {
    'host': 'mysql',
    'port': 3306,
    'user': 'root',
    'password': 'admin',
    'database': 'olist_db',
    'autocommit': True
}

NUM_RECORDS = 50


def get_connection():
    return pymysql.connect(**DB_CONFIG)


def run_bulk_demo():
    conn = get_connection()
    cursor = conn.cursor()

    logging.info(f" BẮT ĐẦU DỘI BOM {NUM_RECORDS} DÒNG CHO MỖI THAO TÁC...\n")

    # 1. BẢNG CUSTOMERS (INCREMENTAL LUỒNG BATCH)
    logging.info("--- Đang xử lý bảng CUSTOMERS ---")

    new_customers = [(uuid.uuid4().hex, uuid.uuid4().hex, str(random.randint(10000, 99999)), 'ho chi minh', 'SG') for _
                     in range(NUM_RECORDS)]
    sql_insert_cust = "INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(sql_insert_cust, new_customers)
    logging.info(f"  [+] Đã Insert {NUM_RECORDS} khách hàng mới.")

    cursor.execute(f"SELECT customer_id FROM customers LIMIT {NUM_RECORDS}")
    cust_to_update = [(c[0],) for c in cursor.fetchall()]
    sql_update_cust = "UPDATE customers SET customer_city = 'da nang', customer_state = 'DN' WHERE customer_id = %s"
    cursor.executemany(sql_update_cust, cust_to_update)
    logging.info(f"  [*] Đã Update {len(cust_to_update)} khách hàng cũ chuyển nhà ra Đà Nẵng.")

    # 2. BẢNG ORDER_ITEMS (LUỒNG CDC)
    logging.info("\n--- Đang xử lý bảng ORDER_ITEMS (CDC) ---")

    # Update giá
    cursor.execute(f"SELECT order_id, order_item_id FROM order_items LIMIT {NUM_RECORDS}")
    items_to_update = [(item[0], item[1]) for item in cursor.fetchall()]
    sql_update_item = "UPDATE order_items SET price = price * 1.10 WHERE order_id = %s AND order_item_id = %s"
    cursor.executemany(sql_update_item, items_to_update)
    logging.info(f"  [*] Đã Update tăng giá 10% cho {len(items_to_update)} món hàng.")

    # Hard Delete Món Hàng (Lấy ID cất đi để lát xóa đơn hàng)
    cursor.execute(f"SELECT order_id, order_item_id FROM order_items LIMIT {NUM_RECORDS} OFFSET 200")
    items_to_delete = [(item[0], item[1]) for item in cursor.fetchall()]
    sql_delete_item = "DELETE FROM order_items WHERE order_id = %s AND order_item_id = %s"
    cursor.executemany(sql_delete_item, items_to_delete)
    logging.info(f"  [-] Đã HARD DELETE {len(items_to_delete)} món hàng.")

    # 3. BẢNG ORDERS (LUỒNG CDC)
    logging.info("\n--- Đang xử lý bảng ORDERS (CDC) ---")
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Tạo đơn hàng mới
    new_orders = [(uuid.uuid4().hex, cust[0], 'created', current_time) for cust in new_customers]
    sql_insert_order = "INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql_insert_order, new_orders)
    logging.info(f"  [+] Đã Insert {NUM_RECORDS} đơn hàng mới.")

    # Update trạng thái
    cursor.execute(f"SELECT order_id FROM orders WHERE order_status != 'delivered' LIMIT {NUM_RECORDS}")
    orders_to_update = [(current_time, o[0]) for o in cursor.fetchall()]
    sql_update_order = "UPDATE orders SET order_status = 'delivered', order_delivered_customer_date = %s WHERE order_id = %s"
    cursor.executemany(sql_update_order, orders_to_update)
    logging.info(f"  [*] Đã Update {len(orders_to_update)} đơn hàng thành 'delivered'.")

    # 4. BẢNG ORDER_REVIEWS (LUỒNG BATCH INCREMENTAL)
    logging.info("\n--- Đang xử lý bảng ORDER_REVIEWS (Batch) ---")

    # Bắn 50 cái review 5 sao cho 50 cái đơn hàng mới vừa được tạo ở trên
    new_reviews = [
        (uuid.uuid4().hex, order[0], 5, 'Dịch vụ quá tuyệt vời, 10 điểm!', current_time)
        for order in new_orders
    ]
    sql_insert_review = "INSERT INTO order_reviews (review_id, order_id, review_score, review_comment_message, review_creation_date) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(sql_insert_review, new_reviews)
    logging.info(f"  [+] Đã Insert {NUM_RECORDS} đánh giá (review) mới.")

    # Giả lập Admin vào trả lời đánh giá (Cập nhật cột review_answer_timestamp để test luồng Incremental lấy data mới)
    cursor.execute(f"SELECT review_id FROM order_reviews LIMIT {NUM_RECORDS}")
    reviews_to_update = [(current_time, r[0]) for r in cursor.fetchall()]
    sql_update_review = "UPDATE order_reviews SET review_answer_timestamp = %s WHERE review_id = %s"
    cursor.executemany(sql_update_review, reviews_to_update)
    logging.info(f"  [*] Đã Update (trả lời) {len(reviews_to_update)} đánh giá cũ.")

    # Hard Delete Đơn Hàng Mẹ (Dựa trên danh sách các món con vừa bị xóa)
    if items_to_delete:
        # Lấy danh sách order_id duy nhất từ danh sách items đã bị xóa ở trên
        orders_to_delete = list(set([(item[0],) for item in items_to_delete]))

        # PHẢI KIỂM TRA: Đơn hàng này có còn món hàng nào khác không? (Xóa hết con mới được xóa mẹ)
        # Để an toàn, trong môi trường demo, chúng ta dùng lệnh xóa bỏ qua lỗi nếu còn dính Foreign Key
        # Thay vì dùng DELETE FROM, ta sẽ bắt Try-Except để lỡ nó đụng cái đơn còn sót hàng thì không bị sập script
        deleted_count = 0
        for order in orders_to_delete:
            try:
                cursor.execute("DELETE FROM orders WHERE order_id = %s", order)
                deleted_count += 1
            except pymysql.err.IntegrityError:
                pass  # Nếu còn dính hàng con khác thì bỏ qua đơn này, không xóa mẹ nữa

        logging.info(f"  [-] Đã HARD DELETE {deleted_count} đơn hàng (sau khi dọn dẹp các món con).")

    conn.commit()
    conn.close()
    logging.info("\nChange data successfully committed.")


if __name__ == "__main__":
    run_bulk_demo()