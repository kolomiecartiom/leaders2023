'''
Даг для выполнения всего ETL-процесса.
'''

import datetime as dt
from airflow import DAG

from leaders2023.helpers import (step, trigger_dag)

default_args = {
    'owner': 'airflow',
    'description': 'common dag',
    'depend_on_past': False,
    'start_date': dt.datetime(2022, 8, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'leaders2023_common'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False,
         tags=['leaders2023'], doc_md=__doc__) as dag:
    step('start_load') >> trigger_dag('leaders2023_stg_xls') >> trigger_dag('leaders2023_ds') >> \
    trigger_dag('leaders2023_pull_sf') >> trigger_dag('leaders2023_dm') >> step('finish_load')

