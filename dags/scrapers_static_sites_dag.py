"""
Scrapers Parallel Market Static DAG
"""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

from datetime import datetime, timedelta

from scrapers.config import post_to_database, upload_to_s3

# Parallel Market import
from scrapers import (
    zanotto_run,
    gigante_autopecas_run,
    rodoponta_run,
    ja_cotei_run,
    scania_ml_run,
    mega_diesel_run,
    mundo_caminhao_run,
    loja_dr3_run,
    vargas_parts_run,
)

# Automakers import
from scrapers import (
    volvo_pecas_run,
    volvo_ml_run,
    iveco_ml_run,
    volkswagwn_ml_run,
)


"""
DAG config
"""
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 22, 5, 0, 0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="scraper_static_sites_dag",
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval="0 8 * * 1",
    tags=["scrapers", "static", "parallel_market", "automakers"],
)


"""
Tasks
"""
scrapers_begin_task = DummyOperator(task_id="scrapers_begin_task", dag=dag)

# Parallel Market Tasks
zanotto_scraper_task = PythonOperator(
    task_id="zanotto_scraper_task", python_callable=zanotto_run, dag=dag
)

gigante_scraper_task = PythonOperator(
    task_id="gigante_scraper_task", python_callable=gigante_autopecas_run, dag=dag
)

rodoponta_scraper_task = PythonOperator(
    task_id="rodoponta_scraper_task", python_callable=rodoponta_run, dag=dag
)

ja_cotei_scraper_task = PythonOperator(
    task_id="ja_cotei_scraper_task", python_callable=ja_cotei_run, dag=dag
)

scania_ml_scraper_task = PythonOperator(
    task_id="scania_ml_scraper_task", python_callable=scania_ml_run, dag=dag
)

mega_diesel_scraper_task = PythonOperator(
    task_id="mega_diesel_scraper_task", python_callable=mega_diesel_run, dag=dag
)

mundo_caminhao_scraper_task = PythonOperator(
    task_id="mundo_caminhao_scraper_task", python_callable=mundo_caminhao_run, dag=dag
)

loja_dr3_scraper_task = PythonOperator(
    task_id="loja_dr3_scraper_task", python_callable=loja_dr3_run, dag=dag
)

vargas_parts_scraper_task = PythonOperator(
    task_id="vargas_parts_scraper_task", python_callable=vargas_parts_run, dag=dag
)

parallel_market_end_task = DummyOperator(task_id="parallel_market_end_task", dag=dag)

# Automakers Tasks
volvo_pecas_scraper_task = PythonOperator(
    task_id="volvo_pecas_scraper_task", python_callable=volvo_pecas_run, dag=dag
)

volvo_ml_scraper_task = PythonOperator(
    task_id="volvo_ml_scraper_task", python_callable=volvo_ml_run, dag=dag
)

iveco_ml_scraper_task = PythonOperator(
    task_id="iveco_ml_scraper_task", python_callable=iveco_ml_run, dag=dag
)

volkswagen_ml_scraper_task = PythonOperator(
    task_id="volkswagen_ml_scraper_task", python_callable=volkswagwn_ml_run, dag=dag
)

automakers_end_task = DummyOperator(task_id="automakers_end_task", dag=dag)

# Upload files data to Databases
post_to_dev_db_task = PythonOperator(
    task_id="post_to_dev_db_task",
    python_callable=post_to_database,
    op_kwargs={"conn_id": "sys_tactics_dev_airflow"},
    dag=dag,
)

post_to_stg_db_task = PythonOperator(
    task_id="post_to_stg_db_task",
    python_callable=post_to_database,
    op_kwargs={"conn_id": "sys_tactics_stg_airflow"},
    dag=dag,
)

post_to_prd_db_task = PythonOperator(
    task_id="post_to_prd_db_task",
    python_callable=post_to_database,
    op_kwargs={"conn_id": "sys_tactics_prd_airflow"},
    dag=dag,
)

# Upload files to S3
upload_to_s3_task = PythonOperator(
    task_id="upload_to_s3_task", python_callable=upload_to_s3, dag=dag
)

scrapers_end_task = DummyOperator(task_id="scrapers_end_task", dag=dag)


"""
Task flow
"""
parallel_market_tasks = [
    zanotto_scraper_task,
    gigante_scraper_task,
    rodoponta_scraper_task,
    ja_cotei_scraper_task,
    scania_ml_scraper_task,
    mega_diesel_scraper_task,
    mundo_caminhao_scraper_task,
    loja_dr3_scraper_task,
    vargas_parts_scraper_task,
]

automakers_tasks = [
    volvo_pecas_scraper_task,
    volvo_ml_scraper_task,
    volkswagen_ml_scraper_task,
    iveco_ml_scraper_task,
]

post_to_db_tasks = [
    post_to_dev_db_task,
    post_to_stg_db_task,
    post_to_prd_db_task,
]

scrapers_begin_task >> parallel_market_tasks >> parallel_market_end_task
parallel_market_end_task >> automakers_tasks >> automakers_end_task
automakers_end_task >> post_to_db_tasks >> upload_to_s3_task >> scrapers_end_task
