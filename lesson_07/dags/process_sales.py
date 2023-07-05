from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.exceptions import AirflowException
import json
import os
from io import BytesIO
from fastavro import writer, parse_schema
from typing import List

RAW_DIR = '/opt/airflow/data/raw/'
STG_DIR = '/opt/airflow/data/stg/'

default_args = {
    'start_date': '2022-08-09',
    'end_date': '2022-08-11',
    'catchup': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'process_sales',
    default_args=default_args,
)


def extract_data_from_api_callback(**context):
    date = context['ds']
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    date = date_obj.strftime('%Y-%m-%d')

    page = 1

    while True:
        http_request_operator = SimpleHttpOperator(
            task_id=f'http_request_operator_page_{page}',
            http_conn_id='fake_api_http',
            headers={'Authorization': '2b8d97ce57d401abd89f45b0079d8790edd940e6'},
            endpoint='sales',
            data={'date': date, 'page': page},
            method='GET',
            response_filter=lambda response: json.loads(response.text),
            log_response=False,
            dag=dag,
        )

        try:
            response = http_request_operator.execute(context=context)
        except AirflowException as e:
            print(f"Error occurred: {str(e)}")
            break

        print(f"Response OK for page {page}")

        file_name = f'sales_{date}_{page}.json'
        raw_dir = os.path.join(RAW_DIR, date)
        os.makedirs(raw_dir, exist_ok=True)

        with open(os.path.join(raw_dir, file_name), 'w') as f:
            json.dump(response, f)

        page += 1


def convert_to_avro_callback(**context):
    date = context['ds']
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    date = date_obj.strftime('%Y-%m-%d')
    try:

        file_list = get_json_files(os.path.join(RAW_DIR, date))

        print(file_list)

        for file_path in file_list:
            with open(file_path, 'r') as file:
                json_data = file.read()

            avro_data = convert_json_to_avro(json_data)
            file_name = os.path.basename(file.name)
            print(file_name)
            stg_file_path = get_stg_file_path(file_name, date, STG_DIR)

            write_avro_file(avro_data, stg_file_path)

        print('Data processing completed')

    except Exception as e:
        print(f"An error occurred: {str(e)}")


def get_json_files(directory: str) -> List[str]:
    json_files = []
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        if os.path.isfile(file_path) and file_name.lower().endswith('.json'):
            json_files.append(file_path)
    return json_files


def convert_json_to_avro(json_data: str) -> bytes:
    schema = {
        "type": "record",
        "name": "Sales",
        "fields": [
            {"name": "client", "type": "string", "default": ""},
            {"name": "purchase_date", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "price", "type": "int"},
        ]
    }

    parsed_schema = parse_schema(schema)
    records = json.loads(json_data)

    with BytesIO() as avro_buffer:
        writer(avro_buffer, parsed_schema, records)
        avro_data = avro_buffer.getvalue()

    return avro_data


def get_stg_file_path(file_name: str, date: str, stg_dir: str) -> str:
    name_without_extension = os.path.splitext(file_name)[0]
    stg_file_path = os.path.join(stg_dir, date, name_without_extension + '.avro')

    return stg_file_path


def write_avro_file(avro_data: bytes, file_path: str) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'wb') as avro_file:
        avro_file.write(avro_data)


extract_data_from_api = PythonOperator(
    task_id='extract_data_from_api',
    python_callable=extract_data_from_api_callback,
    provide_context=True,
    dag=dag,
)

convert_to_avro = PythonOperator(
    task_id='convert_to_avro',
    python_callable=convert_to_avro_callback,
    provide_context=True,
    dag=dag,
)

extract_data_from_api >> convert_to_avro
