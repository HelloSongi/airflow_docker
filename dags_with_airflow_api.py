from datetime import datetime, timedelta
from airflow import DAG



default_args = {
    'owner': 'hellosongi',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}




@dag(
    dag_id = 'dags_with_taskflow_api',
    default_args = default_args,
    description = 'dags using taskflow api',
    start_date = datetime(2022, 7,25, 2),
    schedule_interval = '@daily'
) 


def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {'first_name':'Jerry',
                'last_name':'Fridman'
        }

    @task()
    def get_age():
            return 19

    @task()
    def greet(first_name, last_name, age):
        print(f'Hello World!  My name is {name} '
        f'and i am {age} years old')

    name = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'] age= age)

greet_dag = hello_world_etl()



