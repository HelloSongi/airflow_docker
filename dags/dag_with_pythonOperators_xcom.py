from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator




def greet(ti):
    first_name = ti.xcom_pull(task_id = 'get_name', key='first_name')
    last_name = ti.xcom_pull(task_id = 'get_name', key='last_name')
    age = ti.xcom_pull(task_id = 'get_age', key='age')
    print(f'hello World! My name is {first_name} {last_name}'
    f'and I am {age} years old!')

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value=19)


def get_age(ti):
    ti.xcom_push(key='age', value='Jerry')


default_args = {
    'owner': 'hellosongi',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'python_dags',
    default_args = default_args,
    description = 'dags using python operators',
    start_date = datetime(2022, 7,25, 2),
    schedule_interval = '@daily'
) as dag:

    task1 = PythonOperator(
        task_id = 'greet',
        python_command = greet
    )

    task2 = PythonOperator(
        task_id = 'get_name',
        python_command = get_name
    )

    task3 = PythonOperator(
        task_id = 'get_age',
        python_command = get_age
    )



 


    #method 1
    #task1.set_downstream(task1)
    #task1.set_downstream(task3)

    #method 2
    #task1 >> task2
    #task1 >>task3

    #method 3
    [task2, task3] >> task1