"""
DAG to update database 'raw_data' schema tables
"""
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from airflow import DAG

from scania_etls.raw_data_script import (
    update_raw_fleet,
    update_raw_price_list_bu,
    update_raw_price_list_sm,
    update_raw_transfer_price,
)


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
    dag_id="dev_update_raw_data_dag",
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval="0 8 * * 3",  # toda quarta às 8 da manhã
    tags=["database", "etl", "raw_data", "dev"],
)


"""
Tasks
"""
update_raw_fleet_task = PythonOperator(
    task_id="update_raw_fleet_task",
    python_callable=update_raw_fleet,
    op_kwargs={"conn_id": "sys_tactics_dev_airflow"},
    dag=dag,
)

update_raw_price_list_bu_task = PythonOperator(
    task_id="update_raw_price_list_bu_task",
    python_callable=update_raw_price_list_bu,
    op_kwargs={"conn_id": "sys_tactics_dev_airflow"},
    dag=dag,
)

update_raw_price_list_sm_task = PythonOperator(
    task_id="update_raw_price_list_sm_task",
    python_callable=update_raw_price_list_sm,
    op_kwargs={"conn_id": "sys_tactics_dev_airflow"},
    dag=dag,
)

update_raw_transfer_price_task = PythonOperator(
    task_id="update_raw_transfer_price_task",
    python_callable=update_raw_transfer_price,
    op_kwargs={"conn_id": "sys_tactics_dev_airflow"},
    dag=dag,
)


"""
Task flow - tasks em execução sequencial sem interdependência
"""
update_raw_fleet_task
update_raw_price_list_bu_task
update_raw_price_list_sm_task
update_raw_transfer_price_task
