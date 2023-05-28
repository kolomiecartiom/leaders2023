'''
### Даг для загрузки данных из STG в DS
'''


from datetime import datetime

from airflow import DAG
from leaders2023.helpers import (step, run_sql)

default_args = {
    'owner': 'airflow',
    'description': 'leaders2023_ds',
    'depend_on_past': False,
    'start_date': datetime(2023, 4, 24),
    'email_on_failure': False,
    'on_failure_callback': None,
    'email_on_retry': False,
    'retries': 3
}

dag_id = 'leaders2023_ds'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False,
         tags=['leaders2023'], doc_md=__doc__) as dag:
    t_start_load = step('start_load')
    t_finish_load = step('finish_load')

    t_d_mkb = run_sql(script=f'ds/d_mkb.sql')
    t_d_mkb_standart = run_sql(script=f'ds/d_mkb_standart.sql')
    t_d_package = run_sql(script=f'ds/d_package.sql')
    t_d_package_prescription = run_sql(script=f'ds/d_package_prescription.sql')
    t_d_prescription = run_sql(script=f'ds/d_prescription.sql')
    t_d_standart = run_sql(script=f'ds/d_standart.sql')
    t_d_standart_prescription = run_sql(script=f'ds/d_standart_prescription.sql')
    t_prescriptions = run_sql(script=f'ds/prescriptions.sql')
    t_protocols = run_sql(script=f'ds/protocols.sql')
    t_truncate_stg = run_sql(script='stg1_xls/truncate.sql')

    t_start_load >> t_d_mkb >> t_finish_load
    t_start_load >> t_d_prescription >> t_finish_load
    t_start_load >> t_d_package >> t_d_package_prescription >> t_finish_load
    t_start_load >> t_d_standart >> [t_d_mkb_standart, t_d_standart_prescription] >> t_finish_load
    t_start_load >> t_protocols >> t_prescriptions >> t_truncate_stg >> t_finish_load