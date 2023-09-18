"""
ETLs after the execution of the model
"""
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

from datetime import datetime, timedelta

from scania_etls.raw_data_script import update_raw_corrective_parts_and_fleet
from scania_etls.public_script import (
    update_model_prediction_part_results,
    update_margin_and_price_calculations,
    update_model_prediction_prg_results,
)


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
    dag_id="stg_etl_after_model_dag",
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval=None,
    tags=["connection", "database", "etl"],
)


"""
Tasks
"""
begin_task = DummyOperator(task_id="begin_task", dag=dag)

dummy_task = DummyOperator(task_id="dummy_task", dag=dag)

el_model_prediction_part_results_task = PythonOperator(
    task_id="el_model_prediction_part_results_task",
    python_callable=update_model_prediction_part_results,
    op_kwargs={"conn_id": "sys_tactics_stg_airflow"},
    dag=dag,
)

etl_margin_price_calculations_task = PythonOperator(
    task_id="etl_margin_price_calculations_task",
    python_callable=update_margin_and_price_calculations,
    op_kwargs={"conn_id": "sys_tactics_stg_airflow"},
    dag=dag,
)

etl_update_market_price_task = PostgresOperator(
    task_id="etl_update_market_price_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_update_market_price.sql",
    dag=dag,
)

etl_update_scania_parts_tab_history_task = PostgresOperator(
    task_id="etl_update_scania_parts_tab_history_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_update_scania_parts_tab_history.sql",
    dag=dag,
)

etl_scania_parts_tab_history_task = PostgresOperator(
    task_id="etl_scania_parts_tab_history_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_scania_parts_tab_history.sql",
    dag=dag,
)

etl_scania_parts_tab_task = PostgresOperator(
    task_id="etl_scania_parts_tab_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_scania_parts_tab.sql",
    dag=dag,
)

etl_campaign_history_task = PostgresOperator(
    task_id="etl_campaign_history_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_campaign_history.sql",
    dag=dag,
)

etl_model_prediction_prg_results_task = PythonOperator(
    task_id="etl_model_prediction_prg_results_task",
    python_callable=update_model_prediction_prg_results,
    op_kwargs={"conn_id": "sys_tactics_stg_airflow"},
    dag=dag,
)

etl_scania_prg_tab_task = PostgresOperator(
    task_id="etl_scania_prg_tab_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_scania_prg_tab.sql",
    dag=dag,
)

etl_view_fleet_task = PostgresOperator(
    task_id="etl_view_fleet_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_view_fleet.sql",
    dag=dag,
)

etl_view_pms_market_size_task = PostgresOperator(
    task_id="etl_view_pms_market_size_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_view_pms_market_size.sql",
    dag=dag,
)

etl_raw_corrective_parts_and_fleet_task = PythonOperator(
    task_id="etl_raw_corrective_parts_and_fleet_task",
    python_callable=update_raw_corrective_parts_and_fleet,
    op_kwargs={"conn_id": "sys_tactics_stg_airflow"},
    dag=dag,
)

etl_view_corrective_parts_and_fleet_task = PostgresOperator(
    task_id="etl_view_corrective_parts_and_fleet_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_view_corrective_parts_and_fleet.sql",
    dag=dag,
)

etl_view_monthly_scraper_metrics_task = PostgresOperator(
    task_id="etl_view_monthly_scraper_metrics_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_view_monthly_scraper_metrics.sql",
    dag=dag,
)

etl_view_total_scraper_metrics_task = PostgresOperator(
    task_id="etl_view_total_scraper_metrics_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_view_total_scraper_metrics.sql",
    dag=dag,
)

etl_view_price_mismatch_month_task = PostgresOperator(
    task_id="etl_view_price_mismatch_month_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_view_price_mismatch_month.sql",
    dag=dag,
)

etl_view_revenue_variation_task = PostgresOperator(
    task_id="etl_view_revenue_variation_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_view_revenue_variation.sql",
    dag=dag,
)

etl_cleaning_scania_parts_tab_task = PostgresOperator(
    task_id="etl_cleaning_scania_parts_tab_task",
    postgres_conn_id="sys_tactics_stg_airflow",
    sql="scania_etls/public_sql/etl_cleaning_scania_parts_tab.sql",
    dag=dag,
)

etl_campaign_metrics_task = PostgresOperator(
    task_id="etl_campaign_metrics_task",
    postgres_conn_id="sys_tactics_dev_airflow",
    sql="scania_etls/public_sql/etl_campaign_metrics.sql",
    dag=dag,
)

"""
Task flow
"""
(
    begin_task
    >> el_model_prediction_part_results_task
    >> etl_margin_price_calculations_task
    >> etl_update_scania_parts_tab_history_task
    >> etl_scania_parts_tab_history_task
    >> etl_update_market_price_task
    >> etl_cleaning_scania_parts_tab_task
    >> etl_scania_parts_tab_task
    >> [
        etl_view_revenue_variation_task,
        etl_model_prediction_prg_results_task,
        etl_view_fleet_task,
    ]
    >> dummy_task
    >> [
        etl_view_monthly_scraper_metrics_task,
        etl_view_total_scraper_metrics_task,
        etl_view_price_mismatch_month_task,
    ]
)

etl_model_prediction_prg_results_task >> etl_scania_prg_tab_task
etl_view_fleet_task >> [
    etl_view_pms_market_size_task,
    etl_raw_corrective_parts_and_fleet_task,
]
etl_raw_corrective_parts_and_fleet_task >> etl_view_corrective_parts_and_fleet_task
etl_scania_parts_tab_task >> etl_campaign_history_task >> etl_campaign_metrics_task
