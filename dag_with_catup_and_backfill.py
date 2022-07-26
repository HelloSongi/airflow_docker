from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator



default_args = {
    'owner': 'hellosongi',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}




with DAG(
    dag_id = 'dags_with_ catch and backfill',
    default_args = default_args,
    description = 'dags using taskflow api',
    start_date = datetime(2022, 7,25, 2),
    schedule_interval = '@daily'
    catchup = True
) as dag:

    task1 = BashOperator(
        task_id = 'task1',
        bash_command = 'echo hello world'
    )




