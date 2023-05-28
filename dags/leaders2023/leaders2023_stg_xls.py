'''
### Даг для загрузки данных из источника Excel в STG
'''

from datetime import datetime

from airflow import DAG
from leaders2023.helpers import (step, extract_excel)

default_args = {
    'owner': 'airflow',
    'description': 'leaders2023_stg1_xls',
    'depend_on_past': False,
    'start_date': datetime(2022, 6, 2),
    'email_on_failure': False,
    'on_failure_callback': None,
    'email_on_retry': False,
    'retries': 3
}

dag_id = 'leaders2023_stg1_xls'

xls_entities = [
    'doc_prescriptions',
    'd_package_prescription'
]

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False,
         tags=['leaders2023'], doc_md=__doc__) as dag:
    step('start_load') >> [extract_excel(
        subdir='',
        schema='stg_xls',
        entity=entity,
        from_dir=entity
    ) for entity in xls_entities] >> step('finish_load')