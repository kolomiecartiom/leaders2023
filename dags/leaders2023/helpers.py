from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from plugins.common.helpers import serialize_connection
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# конфигурационный файл проекта
import leaders2023.config as config

# переменные окружения передаются в Docker-контейнер
environment = {
    'AF_EXECUTION_DATE': '{{ ds }}',
    'AF_START_DATE': '{{ dag_run.start_date }}',
    'AF_DAG_ID': '{{ dag.dag_id }}',
    'AF_TASK_ID': '{{ task.task_id }}',
    'AF_TASK_OWNER': '{{ task.owner }}',
    'AF_RUN_ID': '{{ run_id }}',
    'AF_LOGLEVEL': config.log_level,
}


sql_path = f'/app/ws/source/{config.project_dir}/sql'
python_path = f'/app/ws/source/{config.project_dir}/python'
metadata_path = f'/app/ws/metadata/{config.project_dir}'
share_path = f'/app/ws/share/{config.project_dir}/to_process'
cache_path = f'/app/ws/cache/{config.project_dir}/to_process'


def run_sql(script, *, params={}, use_short=False, task_id=None, trigger_rule='all_success', n_retries=3):
    """ Возвращает таск запуска sql-скрипта
            param: script - путь к sql-скрипту относительно каталога sql_path
            param: params - словарь переменных, добавляемых в переменные окружения и доступных шаблонизатору для вставки в sql-скрипт
            param: use_short - признак использования для генерации имени таска только имени скрипта без пути, для скриптов с совпадающими именами из разных папок выставлять false
            param: task_id - наименование таска, если не задано, то генерируется на основе script

    """

    if not task_id:
        if use_short:
            task_id = script.split('/')[-1].rsplit('.', 1)[0]
        else:
            task_id = script.replace('/', '_').rsplit('.', 1)[0]
        task_id = f'sql_{task_id}'
    else:
        task_id = str(task_id or '')

    return DockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': f'{sql_path}/{script}',
                        'AF_DWH_DB_CONNECTION': serialize_connection(config.dwh_db_conn)},
                     **params
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        trigger_rule=trigger_rule,
        retries=n_retries
    )


def extract_excel(subdir, schema, entity, from_dir, params={}, task_id=None, n_retries=0):
    """ Возвращает таск извлечения данных из файлов Excel (включая csv)
            param: subdir - относительный путь к каталогу метаданных
            param: schema - схема целевой таблицы
            param: entity - целевая сущность, используется для определения имен файлов метаданных
            param: from_dir - каталог с файлами для извлечения данных
            param: task_id - наименование таска, если не задано, то генерируется на основе entity
    """
    if not task_id:
        task_id = f'extract_excel_{entity}'
    else:
        task_id = str(task_id or '')

    return DockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION': serialize_connection(config.dwh_db_conn),
                        'AF_PRODUCER': f'{metadata_path}/{subdir}/{entity}_producer.json',
                        'AF_CONSUMER': f'{metadata_path}/{subdir}/{entity}_consumer.json',
                        'AF_FILEPATH': f'{share_path}/{from_dir}',
                        'AF_DWH_DB_SCHEMA': schema},
                        **params
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        retries=n_retries
    )


def run_python(script, *, params={}, task_id=None, use_short=False, trigger_rule='all_success'):
    if not task_id:
        if use_short:
            task_id = script.split('/')[-1].rsplit('.', 1)[0]
        else:
            task_id = script.replace('/', '_').rsplit('.', 1)[0]
        task_id = f'py_{task_id}'
    else:
        task_id = str(task_id or '')

    return DockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': f'{python_path}/{script}',
                        'AF_DWH_DB_CONNECTION': serialize_connection(config.dwh_db_conn)},
                     **params,
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        trigger_rule=trigger_rule
    )


def step(task_id, trigger_rule='all_success'):
    return DummyOperator(
        task_id=task_id, trigger_rule=trigger_rule
    )


def trigger_dag(target_dag_id, conf={}, trigger_rule='all_success'):
    return TriggerDagRunOperator(
        task_id=f'trigger_dag_{target_dag_id}',
        trigger_dag_id=target_dag_id,
        # execution_date='{{ macros.dateutil.parser.parse(ts) }}',
        conf=conf,
        wait_for_completion=True,
        poke_interval=20,
        trigger_rule=trigger_rule
    )