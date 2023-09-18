"""
DAG to update database 'raw_sales_rede' table
"""
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

from scania_etls.raw_data_script import update_raw_sales_rede


"""
DAG config
"""
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="stg_update_raw_sales_rede_dag",
    default_args=DEFAULT_ARGS,
    catchup=False,
    # schedule_interval="0 8 * * 3",  # toda quarta às 8 da manhã
    schedule_interval=None,  # toda quarta às 8 da manhã
    tags=["database", "etl", "raw_sales_rede", "stg"],
)


"""
Tasks
"""
update_raw_sales_rede_task = PythonOperator(
    task_id="update_raw_sales_rede_task",
    python_callable=update_raw_sales_rede,
    op_kwargs={"conn_id": "sys_tactics_stg_airflow"},
    dag=dag,
)


"""
Task flow
"""
update_raw_sales_rede_task
