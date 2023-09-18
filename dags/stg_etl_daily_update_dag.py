"""
ETLs before execution of model (tables before model)
"""
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    dag_id="stg_etl_daily_update_dag",
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval="0 10 * * *",
    tags=["connection", "database", "etl"],
)

"""
Tasks
"""

etl_daily_update_time_series = PostgresOperator(
    task_id="etl_daily_update_time_series",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_daily_update_time_series.sql",
    dag=dag,
)

etl_daily_update_scania_parts_tab_history = PostgresOperator(
    task_id="etl_daily_update_scania_parts_tab_history",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_daily_update_scania_parts_tab_history.sql",
    dag=dag,
)

etl_daily_update_part_line3 = PostgresOperator(
    task_id="etl_daily_update_part_line3",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_daily_update_part_line3.sql",
    dag=dag,
)

etl_daily_update_scania_parts_tab = PostgresOperator(
    task_id="etl_daily_update_scania_parts_tab",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_daily_update_scania_parts_tab.sql",
    dag=dag,
)

etl_daily_update_campaign_history = PostgresOperator(
    task_id="etl_daily_update_campaign_history",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_daily_update_campaign_history.sql",
    dag=dag,
)


"""
Task flow
"""
(
    etl_daily_update_time_series
    >> etl_daily_update_scania_parts_tab_history
    >> etl_daily_update_part_line3
    >> etl_daily_update_scania_parts_tab
    >> etl_daily_update_campaign_history
)

