import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from table_defs.customers_csv import customers_bronze_csv

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="customers_pipeline",
    description="Ingest and process customers data",
    schedule_interval='0 7 * * *',
    start_date=dt.datetime(2022, 8, 1),
    end_date=dt.datetime(2022, 8, 3),
    catchup=True,
    tags=['customers'],
    default_args=DEFAULT_ARGS,
)

dag.doc_md = __doc__

transfer_from_data_to_raw = BigQueryInsertJobOperator(
    task_id='transfer_from_data_to_raw',
    dag=dag,
    location='US',
    project_id='de2023-bohdan-ivakhnov',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_from_data_raw_to_bronze_customers.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "customers_csv": customers_bronze_csv,
            },
        }
    },

    params={
        'data_raw_bucket': "test_bucket_ivakhnov",
        'project_id': "de2023-bohdan-ivakhnov"
    }
)

transfer_from_dwh_bronze_to_dwh_silver = BigQueryInsertJobOperator(
    task_id='transfer_from_dwh_bronze_to_dwh_silver',
    dag=dag,
    location='US',
    project_id='de2023-bohdan-ivakhnov',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_from_dwh_bronze_to_dwh_silver_customers.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': "de2023-bohdan-ivakhnov"
    }
)

transfer_from_data_to_raw >> transfer_from_dwh_bronze_to_dwh_silver
