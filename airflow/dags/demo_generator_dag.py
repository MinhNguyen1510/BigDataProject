from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2026, 4, 12),
    'catchup': False
}

with DAG(
    dag_id='00_Demo_Data_Generator',
    default_args=default_args,
    schedule_interval=None,
    tags=['demo', 'mysql', 'generator'],
    description='Giả lập người dùng Insert/Update/Delete data vào MySQL để test CDC'
) as dag:

    generate_demo_data = BashOperator(
        task_id='fire_data_bombs',
        bash_command='python /opt/airflow/etl/tools/bulk_demo_data.py',
    )

    generate_demo_data