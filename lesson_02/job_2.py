import os
import logging
import json
from typing import List
from flask import Flask, request
from io import BytesIO
from fastavro import writer, parse_schema

app = Flask(__name__)

app.logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

app.logger.addHandler(stream_handler)


@app.route('/', methods=['POST'])
def handle_request():
    app.logger.info('test')

    try:
        data = request.json
        raw_dir = data.get('raw_dir')
        stg_dir = data.get('stg_dir')

        if raw_dir is None or stg_dir is None:
            return 'Invalid request data', 400
        raw_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), raw_dir)
        stg_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), stg_dir)

        file_list = get_json_files(raw_dir)

        for file_path in file_list:
            with open(file_path, 'r') as file:
                json_data = file.read()

            avro_data = convert_json_to_avro(json_data)
            file_name = os.path.basename(file.name)
            stg_file_path = get_stg_file_path(file_name, raw_dir, stg_dir)

            write_avro_file(avro_data, stg_file_path)

        return 'Data processing completed', 201

    except Exception as e:
        app.logger.error(f"An error occurred: {str(e)}")
        return f"An error occurred: {str(e)}", 500


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


def get_stg_file_path(file_name: str, raw_dir: str, stg_dir: str) -> str:
    name_without_extension = os.path.splitext(file_name)[0]
    stg_file_path = os.path.join(stg_dir, name_without_extension + '.avro')

    return stg_file_path


def write_avro_file(avro_data: bytes, file_path: str) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'wb') as avro_file:
        avro_file.write(avro_data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082)
