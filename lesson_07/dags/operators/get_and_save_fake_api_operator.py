import os
from typing import Any

from airflow import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import SimpleHttpOperator
import json


class GetAndSaveFakeApiOperator(SimpleHttpOperator):

    def __init__(self, raw_dir, **kwargs: Any):

        self.raw_dir = raw_dir
        super().__init__(**kwargs)

    def execute(self, context):
        from airflow.utils.operator_helpers import determine_kwargs

        http = HttpHook(
            self.method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )

        self.log.info("Calling HTTP method")

        date = context.get('ds')
        page = 1
        while True:
            try:
                response = http.run(
                    self.endpoint,
                    {'date': date, 'page': page},
                    self.headers,
                    self.extra_options
                )

                if self.log_response:
                    self.log.info(response.text)
                if self.response_check:
                    kwargs = determine_kwargs(self.response_check, [response], context)
                    if not self.response_check(response, **kwargs):
                        raise AirflowException("Response check returned False.")

                # print(response.text)
                # exit()
            except AirflowException as e:
                print(f"Error occurred: {str(e)}")
                break

            print(f"Response OK for page {page}")

            file_name = f'sales_{date}_{page}.json'
            raw_dir = os.path.join(self.raw_dir, date)
            os.makedirs(raw_dir, exist_ok=True)

            with open(os.path.join(raw_dir, file_name), 'w') as f:
                json.dump(json.loads(response.text), f)

            page += 1

            # return response.text
