"""
ETLs before execution of model (tables before model)
"""
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

from datetime import datetime, timedelta


"""
DAG config
"""
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 9, 20, 5, 0, 0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="stg_etl_before_model_dag",
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval=None,
    tags=["connection", "database", "etl"],
)


"""
Tasks
"""
begin_task = DummyOperator(task_id="begin_task", dag=dag)

etl_price_list_task = PostgresOperator(
    task_id="etl_price_list_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_price_list.sql",
    dag=dag,
)

etl_description_prg_code_task = PostgresOperator(
    task_id="etl_description_prg_code_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_description_prg_code.sql",
    dag=dag,
)

etl_scania_products_task = PostgresOperator(
    task_id="etl_scania_products_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_scania_products.sql",
    dag=dag,
)

etl_time_series_task = PostgresOperator(
    task_id="etl_time_series_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_time_series.sql",
    dag=dag,
)

refresh_view_percentage_of_products_sold_task = PostgresOperator(
    task_id="refresh_view_percentage_of_products_sold_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_view_percentage_of_products_sold.sql",
    dag=dag,
)

etl_maintenance_percentage_task = PostgresOperator(
    task_id="etl_maintenance_percentage_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_maintenance_percentage.sql",
    dag=dag,
)

etl_competitors_products_task = PostgresOperator(
    task_id="etl_competitors_products_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_competitors_products.sql",
    dag=dag,
)


"""
Task flow
"""
(
    begin_task
    >> etl_competitors_products_task
    >> etl_price_list_task
    >> [
        etl_maintenance_percentage_task,
        etl_description_prg_code_task,
        etl_time_series_task,
    ]
)

etl_description_prg_code_task >> etl_scania_products_task
etl_time_series_task >> refresh_view_percentage_of_products_sold_task
