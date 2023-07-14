import json
import os
from io import BytesIO
from typing import Any, List

from airflow.models import BaseOperator

from fastavro import writer, parse_schema



class ConvertToAvroOperator(BaseOperator):

    def __init__(self, stg_dir, raw_dir, **kwargs):
        super().__init__(**kwargs)
        self.raw_dir = raw_dir
        self.stg_dir = stg_dir

    def execute(self, context) -> Any:
        self.convert_to_avro_callback(context.get('ds'))

    def convert_to_avro_callback(self, date):

        try:

            file_list = self.get_json_files(self, os.path.join(self.raw_dir, date))

            print(file_list)

            for file_path in file_list:
                with open(file_path, 'r') as file:
                    json_data = file.read()

                avro_data = self.convert_json_to_avro(self, json_data)
                file_name = os.path.basename(file.name)
                print(file_name)
                stg_file_path = self.get_stg_file_path(file_name, date, self.stg_dir)

                self.write_avro_file(avro_data, stg_file_path)

            print('Data processing completed')

        except Exception as e:
            print(f"An error occurred: {str(e)}")

    @staticmethod
    def get_json_files(self, directory: str) -> List[str]:
        json_files = []
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path) and file_name.lower().endswith('.json'):
                json_files.append(file_path)
        return json_files

    @staticmethod
    def convert_json_to_avro(self, json_data: str) -> bytes:
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

    @staticmethod
    def get_stg_file_path(file_name: str, date: str, stg_dir: str) -> str:
        name_without_extension = os.path.splitext(file_name)[0]
        stg_file_path = os.path.join(stg_dir, date, name_without_extension + '.avro')

        return stg_file_path

    @staticmethod
    def write_avro_file(avro_data: bytes, file_path: str) -> None:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'wb') as avro_file:
            avro_file.write(avro_data)
