from airflow.decorators import dag,task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


from task_groups.extract_TG import extract
from task_groups.transform_TG import transform
from task_groups.load_TG import load

@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "ecommerce"]
)
def E_Commerce_ETL_DAG():

    extract_tg = extract()

    transform_tg = transform()

    load_tg = load()
        

    extract_tg >> transform_tg >> load_tg
    


E_Commerce_ETL_DAG()
