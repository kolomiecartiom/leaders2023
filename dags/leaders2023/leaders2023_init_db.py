'''
### Даг для инициализации таблиц в DWH
'''


from datetime import datetime

from airflow import DAG
from leaders2023.helpers import (step, run_sql)

default_args = {
    'owner': 'airflow',
    'description': 'leaders2023_init_db',
    'depend_on_past': False,
    'start_date': datetime(2023, 4, 24),
    'email_on_failure': False,
    'on_failure_callback': None,
    'email_on_retry': False,
    'retries': 3
}

dag_id = 'leaders2023_init_db'

schemes = [
    'dm',
    'ds',
    'stg1',
]


with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False,
         tags=['leaders2023'], doc_md=__doc__) as dag:
    step('start_load') >> [
        run_sql(
            script=f'init_db/{scheme}.sql',
            task_id=f'init_{scheme}'
        ) for scheme in schemes
    ] >> step('finish_load')