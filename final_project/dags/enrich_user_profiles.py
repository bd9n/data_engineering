import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago


DEFAULT_ARGS = {
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="enrich_user_profiles_pipeline",
    description="Ingest and process enrich_user_profiles data",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['customers'],
    default_args=DEFAULT_ARGS,
)

dag.doc_md = __doc__

enrich_user_profiles = BigQueryInsertJobOperator(
    task_id='enrich_user_profiles',
    dag=dag,
    location='US',
    project_id='de2023-bohdan-ivakhnov',
    configuration={
        "query": {
            "query": "{% include 'sql/enrich_user_profiles.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': "de2023-bohdan-ivakhnov"
    }
)

enrich_user_profiles
