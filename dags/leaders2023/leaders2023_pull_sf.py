'''
### Даг для загрузки данных из хранилища DWH в SmartForms
'''


from datetime import datetime

from airflow import DAG
from leaders2023.helpers import (step, run_python)
from leaders2023.config import visiology_conn

default_args = {
    'owner': 'airflow',
    'description': 'leaders2023_pull_sf',
    'depend_on_past': False,
    'start_date': datetime(2023, 4, 24),
    'email_on_failure': False,
    'on_failure_callback': None,
    'email_on_retry': False,
    'retries': 3
}

dim_script = [
    'pull_d_prescription_doctor',
    'pull_d_prescription_standart'
]

dag_id = 'leaders2023_pull_sf'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False,
         tags=['leaders2023'], doc_md=__doc__) as dag:
    step('start_load') >> [
        run_python(
            script=f'{script}.py',
            params={
                'AF_VISIOLOGY_CONNECTION': visiology_conn,
            }
        ) for script in dim_script
    ] >> run_python(
        script=f'pull_gp_matches.py',
        params={
            'AF_VISIOLOGY_CONNECTION': visiology_conn,
        }
    ) >> step('finish_load')