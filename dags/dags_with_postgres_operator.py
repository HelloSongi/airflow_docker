from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator



default_args = {
    'owner': 'hellosongi',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}




with DAG(
    dag_id = 'dags_with_postgres_operator',
    default_args = default_args,
    description = 'dags using taskflow api',
    start_date = datetime(2022, 7,25, 2),
    schedule_interval = '@daily'
    catchup = True
) as dag:

    task1 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_locolhost'
        sql="""
            create table if not exists dag_run(
                dt date'
                dag_id character varying,
                primary key(dt, dag-id)
            )
        """
    )

    task2 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_locolhost'
        sql="""
            insert into dag_run(dt, dag_id) 
            values('{{ ds }}', '{{dag.dag_id}}')
        """
    ) 

task1 >> task2




