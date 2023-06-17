import os
import logging
import requests
from flask import Flask, request, make_response

app = Flask(__name__)

AUTH_TOKEN = os.environ['AUTH_TOKEN']

app.logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

app.logger.addHandler(stream_handler)


@app.route('/', methods=['POST'])
def handle_request() -> make_response:
    try:
        data = request.json
        if not data:
            raise ValueError('Empty JSON data in the request')

        date = data.get('date')
        raw_dir = data.get('raw_dir')

        if not date or not raw_dir:
            raise ValueError('Invalid request data')

        app.logger.info(raw_dir)

        raw_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), raw_dir)
        os.makedirs(raw_dir, exist_ok=True)

        clear_directory(raw_dir)

        page = 1
        while True:
            response = requests.get(
                url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
                params={'date': date, 'page': page},
                headers={'Authorization': AUTH_TOKEN},
            )
            if response.ok:
                file_name = f'sales_{date}_{page}.json'
                file_path = os.path.join(raw_dir, file_name)
                with open(file_path, 'w') as f:
                    f.write(response.text)
                page += 1
            else:
                break

        return make_response('Data saved successfully', 201)

    except Exception as e:
        app.logger.error(f'An error occurred: {str(e)}')
        return 'An error occurred during processing'


def clear_directory(directory: str) -> None:
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081)
