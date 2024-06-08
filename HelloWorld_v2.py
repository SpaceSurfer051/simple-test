from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from plugins import slack
from airflow.macros import *

@task
def print_hello():
    print("hello!")
    return "hello!"

@task
def print_goodbye():
    print("goodbye!")
    return "goodbye!"

with DAG(
    dag_id = 'HelloWorld_v2',
    start_date = datetime(2022,5,5),
    catchup=False,
    tags=['example'],
    schedule = '0 2 * * *'
    default_args= {
        'on_failure_callback': slack.on_failure_callback,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:

    # Assign the tasks to the DAG in order
    print_hello() >> print_goodbye()
