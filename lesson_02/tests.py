import os
import json
import pytest
from flask import Flask
from job_1 import app, clear_directory
from job_2 import app, get_json_files, convert_json_to_avro, get_stg_file_path, write_avro_file


def test_handle_request_success(monkeypatch, tmpdir):
    raw_dir = tmpdir.mkdir("raw_data")
    date = '2023-06-15'
    data = {'date': date, 'raw_dir': str(raw_dir)}

    def mock_get(*args, **kwargs):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code

            def json(self):
                return self.json_data

            @property
            def ok(self):
                return self.status_code == 200

            @property
            def text(self):
                return str(self.json_data)

        if kwargs['params']['page'] == 1:
            return MockResponse({'data': 'page 1'}, 200)
        elif kwargs['params']['page'] == 2:
            return MockResponse({'data': 'page 2'}, 200)
        else:
            return MockResponse({}, 404)

    monkeypatch.setattr('requests.get', mock_get)

    with app.test_client() as client:
        response = client.post('/', json=data)

        assert response.status_code == 201
        assert response.data == b'Data saved successfully'
        dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), raw_dir)
        assert os.path.isfile(os.path.join(dir, f'sales_{date}_1.json'))
        assert os.path.isfile(os.path.join(dir, f'sales_{date}_2.json'))


def test_handle_request_error(monkeypatch):
    data = {}

    def mock_get(*args, **kwargs):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code

            def json(self):
                return self.json_data

            @property
            def ok(self):
                return self.status_code == 200

            @property
            def text(self):
                return str(self.json_data)

        return MockResponse({}, 404)

    monkeypatch.setattr('requests.get', mock_get)

    with app.test_client() as client:
        response = client.post('/', json=data)

        assert response.status_code == 20
        assert response.data == b'An error occurred during processing'


def test_clear_directory(tmpdir):
    directory = str(tmpdir)
    file1 = tmpdir.join("file1.txt")
    file1.write("test")
    file2 = tmpdir.join("file2.txt")
    file2.write("test")

    clear_directory(directory)

    assert not os.path.exists(file1)
    assert not os.path.exists(file2)


@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


def test_get_json_files():
    directory = 'test_data'
    expected_files = ['file1.json', 'file2.json']
    expected_files = [os.path.join(directory, file) for file in expected_files]

    files = get_json_files(directory)

    assert files == expected_files


def test_convert_json_to_avro():
    json_data = '[{"client": "John", "purchase_date": "2023-06-15", "product": "Item", "price": 10}]'
    expected_avro_data = b'\x00\x01{"client": "John", "purchase_date": "2023-06-15", "product": "Item", "price": 10}'

    avro_data = convert_json_to_avro(json_data)

    assert avro_data == expected_avro_data


def test_get_stg_file_path():
    file_name = 'data.json'
    raw_dir = 'raw'
    stg_dir = 'stg'
    expected_file_path = os.path.join(stg_dir, 'data.avro')

    file_path = get_stg_file_path(file_name, raw_dir, stg_dir)

    assert file_path == expected_file_path


def test_write_avro_file():
    avro_data = b'\x00\x01{"client": "John", "purchase_date": "2023-06-15", "product": "Item", "price": 10}'
    file_path = 'test_data/data.avro'
    expected_file_content = avro_data

    write_avro_file(avro_data, file_path)

    with open(file_path, 'rb') as f:
        file_content = f.read()

    assert file_content == expected_file_content
