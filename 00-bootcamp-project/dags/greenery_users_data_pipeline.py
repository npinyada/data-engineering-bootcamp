import csv
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
PROJECT_ID = "kinetic-harbor-384413"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "users"


def _extract_data(ds):
    url = f"http://34.87.139.82:8000/{DATA}/?created_at={ds}"
    response = requests.get(url)
    data = response.json()

    if data:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
                "user_id",
                "first_name",
                "last_name",
                "email",
                "phone_number",
                "created_at",
                "updated_at",
                "address",
            ]
            writer.writerow(header)
            for each in data:
                data = [
                    each["user_id"],
                    each["first_name"],
                    each["last_name"],
                    each["email"],
                    each["phone_number"],
                    each["created_at"],
                    each["updated_at"],
                    each["address"]
                ]
                writer.writerow(data)


def _load_data_to_gcs(ds):
    keyfile_gcs = "/opt/airflow/dags/kinetic-harbor-384413-27585638075a_bigquery_and_gcs.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb-bootcamp-100029"
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DAGS_FOLDER}/{DATA}-{ds}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    try:
        blob.upload_from_filename(file_path)
    except FileNotFoundError:
        pass

def _load_data_from_gcs_to_bigquery(ds):
    keyfile_gcs = "/opt/airflow/dags/kinetic-harbor-384413-27585638075a_bigquery_and_gcs.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )
    
    bucket_name = "deb-bootcamp-100029"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    if blob.exists():
        keyfile_bigquery = "/opt/airflow/dags/kinetic-harbor-384413-27585638075a_bigquery_and_gcs.json"
        service_account_info_bigquery = json.load(open(keyfile_bigquery))
        credentials_bigquery = service_account.Credentials.from_service_account_info(
            service_account_info_bigquery
        )

        bigquery_client = bigquery.Client(
            project=PROJECT_ID,
            credentials=credentials_bigquery,
            location=LOCATION,
        )

        partition = ds.replace("-", "")
        table_id = f"{PROJECT_ID}.deb_bootcamp.{DATA}${partition}"
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
            clustering_fields=["first_name", "last_name"]
        )

        bucket_name = "deb-bootcamp-100029"
        destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
        job = bigquery_client.load_table_from_uri(
            f"gs://{bucket_name}/{destination_blob_name}",
            table_id,
            job_config=job_config,
            location=LOCATION,
        )
        job.result()

        table = bigquery_client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}
with DAG(
    dag_id="greenery_users_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery