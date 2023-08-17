import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

from table_defs.user_profiles_jsonl import user_profiles_bronze_jsonl

DEFAULT_ARGS = {
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="user_profiles_pipeline",
    description="Ingest and process user_profiles data",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
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
            "query": "{% include 'sql/transfer_from_data_raw_to_bronze_user_profiles.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "user_profiles_jsonl": user_profiles_bronze_jsonl,
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
            "query": "{% include 'sql/transfer_from_dwh_bronze_to_dwh_silver_user_profiles.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': "de2023-bohdan-ivakhnov"
    }
)

transfer_from_data_to_raw >> transfer_from_dwh_bronze_to_dwh_silver
