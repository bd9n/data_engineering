import os
from datetime import timedelta
from airflow import DAG

from operators.local_folder_to_gcs import UploadFolderToGCSOperator
from operators.get_and_save_fake_api_operator import GetAndSaveFakeApiOperator
from operators.convert_to_avro_operator import ConvertToAvroOperator

RAW_DIR = '/opt/airflow/data/raw/'
STG_DIR = '/opt/airflow/data/stg/'
GCS_DIR = '/src1/sales/v1/'

default_args = {
    'start_date': '2022-08-09',
    'end_date': '2022-08-11',
    'catchup': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
        'process_sales',
        default_args=default_args,
):
    extract_data_from_api = GetAndSaveFakeApiOperator(
        task_id='extract_data_from_api',
        http_conn_id='fake_api_http',
        headers={'Authorization': os.environ.get('FAKE_API_TOKEN')},
        endpoint='sales',
        raw_dir=RAW_DIR,
        method='GET',
    )

    convert_to_avro = ConvertToAvroOperator(
        task_id='convert_to_avro',
        raw_dir=RAW_DIR,
        stg_dir=STG_DIR
    )

    upload_task = UploadFolderToGCSOperator(
        task_id=f'upload_file',
        src_folder=STG_DIR,
        dst_bucket=GCS_DIR,
        bucket='test_bucket_ivakhnov',
        gcp_conn_id='gcs-conn-id'
    )

    extract_data_from_api >> convert_to_avro >> upload_task
