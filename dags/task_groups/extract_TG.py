from airflow.decorators import task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


@task_group
def extract():
    create_schema = SQLExecuteQueryOperator(
            task_id="create_schema_staging",
            conn_id="DWH",
            sql="CREATE SCHEMA IF NOT EXISTS staging;"
        )

    create_sales = SQLExecuteQueryOperator(
            task_id="create_table_sales",
            conn_id="DWH",
            sql="/sql/create/create_staging_sales.sql"
        )

    create_products = SQLExecuteQueryOperator(
            task_id="create_table_products",
            conn_id="DWH",
            sql="/sql/create/create_staging_products.sql"
        )
    
    @task
    def extract_sales():
            src = PostgresHook(postgres_conn_id="Source")
            dest = PostgresHook(postgres_conn_id="DWH")
            

            max_date = Variable.get("MAX_TRANSACTION_DATE", default_var="1900-01-01")
            
            query = f"""
                SELECT *
                FROM public.sales
                WHERE transactional_date > '{max_date}';
            """
            rows = src.get_records(query)
            
            if not rows:
                return "No new data"
            

            dest.insert_rows("staging.sales", rows)
            
            new_max = dest.get_first("SELECT MAX(transactional_date) FROM staging.sales;")[0]
            
            if new_max:  # Avoid overwriting with None
                Variable.set("MAX_TRANSACTION_DATE", str(new_max))



    @task
    def extract_products():
            src = PostgresHook(postgres_conn_id="Source")
            dest = PostgresHook(postgres_conn_id="DWH")
            
            query = f"""
                SELECT *
                FROM public.products;
            """
            rows = src.get_records(query)
            
            if not rows:
                return "No new data"
            
            dest.run("TRUNCATE TABLE staging.products;")
            dest.insert_rows("staging.products", rows)


      
            
    create_schema >> create_sales

    create_schema >> create_products

    create_sales >> extract_sales()

    create_products >> extract_products()
