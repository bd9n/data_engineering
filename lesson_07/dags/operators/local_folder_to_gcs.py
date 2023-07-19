import os

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class UploadFolderToGCSOperator(BaseOperator):
    def __init__(self, src_folder, dst_bucket, bucket, gcp_conn_id: str = "google_cloud_default", **kwargs):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.src_folder = src_folder
        self.dst_bucket = dst_bucket
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        date = context.get('ds')
        execution_date = context.get('data_interval_start')

        year = execution_date.year
        month = execution_date.month
        day = execution_date.day

        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        dest_dir = os.path.join(self.src_folder, date)

        for root, dirs, files in os.walk(dest_dir):

            for file_name in files:
                file_path = os.path.join(root, file_name)
                gcs_object = os.path.join(self.dst_bucket, f'{year}', f'{month}', f'{day}', file_name)
                hook.upload(
                    bucket_name=self.bucket,
                    object_name=gcs_object,
                    filename=file_path,
                )
                self.log.info("File %s uploaded to %s in %s bucket", file_path, gcs_object, self.bucket)
