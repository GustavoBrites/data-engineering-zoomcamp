import os
import logging


from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from ingest_script_hw import ingest_callable

import pyarrow.csv as pv
import pyarrow.parquet as pq

#names GCP
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

#airflow home
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

#DAG dates
hw_workflow = DAG(
    dag_id = "ingestionDag_GREEN",
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 1), 
    catchup=True,
    max_active_runs=3

)

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data' 
csv_file_name = 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
URL_TEMPLATE = URL_PREFIX + '/' + csv_file_name
csv_FILE_path = AIRFLOW_HOME + '/' + csv_file_name
TABLE_NAME_TEMPLATE = 'green_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
parquet_file_name = csv_file_name.replace('.csv', '.parquet')



# def functions
def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

with hw_workflow:
    download_dataset_task = BashOperator(
        task_id='data_download',
        bash_command=f'curl -sSLf {URL_TEMPLATE} > {csv_FILE_path}'
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=csv_FILE_path
        ),
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{csv_FILE_path}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"ingested/{parquet_file_name}",
            "local_file": f"{AIRFLOW_HOME}/{parquet_file_name}",
        },
    )


    download_dataset_task >> ingest_task >> format_to_parquet_task >> local_to_gcs_task