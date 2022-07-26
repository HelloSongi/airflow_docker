from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator




default_args = {
    'owner': 'hellosongi',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'my_first_dag',
    default_args = default_args,
    description = 'this is my first dag that i wrote',
    start_date = datetime(2022, 7,25, 2),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = 'echo hello world'
    )

    task2 = BashOperator(
        task_id = 'second-task',
        bash_command = 'this will run after task 1 is done'
    )

    task3 = BashOperator(
        task_id = 'third_task',
        bash_command = 'hey, i will running after task 1 together with task 2'
    )


    #method 1
    #task1.set_downstream(task1)
    #task1.set_downstream(task3)

    #method 2
    #task1 >> task2
    #task1 >>task3

    #method 3
    task1 >> [task2, task3]