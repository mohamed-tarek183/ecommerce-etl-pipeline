from airflow.decorators import task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task_group
def transform():

    transform_products = SQLExecuteQueryOperator(
            task_id="transform_products",
            conn_id="DWH",
            sql="/sql/transform/transform_products.sql"
        )

    transform_sales = SQLExecuteQueryOperator(
            task_id="transform_sales",
            conn_id="DWH",
            sql="/sql/transform/transform_sales.sql"
        )