from airflow.decorators import task,task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook



@task_group
def load():
        create_core_schema = SQLExecuteQueryOperator(
            task_id="create_schema_core",
            conn_id="DWH",
            sql="CREATE SCHEMA IF NOT EXISTS core;"
        )

        create_dim_products = SQLExecuteQueryOperator(
            task_id="create_dim_products",
            conn_id="DWH",
            sql="sql/create/create_dim_products.sql"
        )

        create_dim_payment = SQLExecuteQueryOperator(
            task_id="create_dim_payment",
            conn_id="DWH",
            sql="sql/create/create_dim_payment.sql"
        )


        create_dim_date = SQLExecuteQueryOperator(
            task_id="create_dim_date",
            conn_id="DWH",
            sql="sql/create/create_dim_date.sql"
        )

        create_fact_sales = SQLExecuteQueryOperator(
            task_id="create_fact_sales",
            conn_id="DWH",
            sql="sql/create/create_fact_sales.sql"
        )

        load_dim_products=SQLExecuteQueryOperator(
            task_id="load_dim_products",
            conn_id="DWH",
            sql="sql/load/load_dim_products.sql"
        )

        load_dim_date=SQLExecuteQueryOperator(
            task_id="load_dim_date",
            conn_id="DWH",
            sql="sql/load/load_dim_date.sql"
        )

        load_dim_payment=SQLExecuteQueryOperator(
            task_id="load_dim_payment",
            conn_id="DWH",
            sql="sql/load/load_dim_payment.sql"
        )

        load_fact_sales=SQLExecuteQueryOperator(
            task_id="load_fact_sales",
            conn_id="DWH",
            sql="sql/load/load_fact_sales.sql"
        )



        create_core_schema >> [create_dim_products,create_dim_payment,create_dim_date]

        create_dim_products >> load_dim_products

        create_dim_payment >> load_dim_payment

        create_dim_date >> load_dim_date

        [load_dim_products,load_dim_payment,load_dim_date] >> create_fact_sales

        create_fact_sales >> load_fact_sales