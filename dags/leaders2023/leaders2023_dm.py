'''
### Даг для загрузки данных из DS в DM
'''

from datetime import datetime

from airflow import DAG
from leaders2023.helpers import (step, run_sql)

default_args = {
    'owner': 'airflow',
    'description': 'leaders2023_dm',
    'depend_on_past': False,
    'start_date': datetime(2023, 4, 24),
    'email_on_failure': False,
    'on_failure_callback': None,
    'email_on_retry': False,
    'retries': 3
}

dag_id = 'leaders2023_dm'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False,
         tags=['leaders2023'], doc_md=__doc__) as dag:
    t_start_load = step('start_load')
    t_finish_load = step('finish_load')

    t_prescriptions = run_sql(script=f'dm/prescriptions.sql')
    t_protocols = run_sql(script=f'dm/protocols.sql')

    t_start_load >> t_prescriptions >> t_protocols >> t_finish_load