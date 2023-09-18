"""
Scrapers Parallel Market Dynamic DAG
"""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

from datetime import datetime, timedelta

from scrapers import connect_parts_run, loja_stemac_run, brasparts_run
from scrapers.config import upload_to_s3


"""
DAG config
"""
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 22, 5, 0, 0),  # datetime(2022, 11, 22, 12, 0, 0)
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="test_scrapers_dynamic_sites_dag",
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval=None,
)


"""
Tasks
"""
scrapers_begin_task = DummyOperator(task_id="scrapers_begin_task", dag=dag)

connect_parts_scraper_task = PythonOperator(
    task_id="connect_parts_scraper_task", python_callable=connect_parts_run, dag=dag
)

loja_stemac_scraper_task = PythonOperator(
    task_id="loja_stemac_scraper_task", python_callable=loja_stemac_run, dag=dag
)

brasparts_scraper_task = PythonOperator(
    task_id="brasparts_scraper_task", python_callable=brasparts_run, dag=dag
)

# upload files to S3
# upload_to_s3_task = PythonOperator(
#     task_id="upload_to_s3_task", python_callable=upload_to_s3, dag=dag
# )

scrapers_end_task = DummyOperator(task_id="scrapers_end_task", dag=dag)


"""
Task flow
"""
parallel_market_tasks = [
    connect_parts_scraper_task,
    brasparts_scraper_task,
    loja_stemac_scraper_task,
]

# scrapers_begin >> parallel_market_tasks >> upload_to_s3_task >> scrapers_end
scrapers_begin_task >> parallel_market_tasks >> scrapers_end_task
