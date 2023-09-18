"""
Execution of model
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
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
    dag_id="run_model_dag",
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval=None,
    tags=["model", "database", "etl"],
)


"""
Tasks
"""
begin_task = DummyOperator(task_id="begin_task", dag=dag)

etl_model_task = BashOperator(
    task_id="etl_model_task",
    dag=dag,
    bash_command="cd /opt/airflow/dags/docker_model/ && python3 run_model.py",
)

end_task = DummyOperator(task_id="end_task", dag=dag)


"""
Task flow
"""
begin_task >> etl_model_task >> end_task
