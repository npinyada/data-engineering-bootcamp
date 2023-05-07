# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

import json
import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "kinetic-harbor-384413"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("first_name", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("last_name", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("email", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("phone_number", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
    ],
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
    clustering_fields=["first_name", "last_name"],
)

file_path = "data/users.csv"
df = pd.read_csv(file_path, parse_dates=["created_at", "updated_at"])
df.info()

table_id = f"{project_id}.deb_bootcamp.users"
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("name", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("price", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("Inventory", bigquery.SqlTypeNames.INTEGER),
    ],
)

file_path = "data/products.csv"
df = pd.read_csv(file_path)
df.info()

table_id = f"{project_id}.deb_bootcamp.products"
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("discount", bigquery.SqlTypeNames.INTEGER),
        bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
    ],
)

file_path = "data/promos.csv"
df = pd.read_csv(file_path)
df.info()

table_id = f"{project_id}.deb_bootcamp.promos"
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("zipcode", bigquery.SqlTypeNames.INTEGER),
        bigquery.SchemaField("state", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("country", bigquery.SqlTypeNames.STRING),
    ],
)

file_path = "data/addresses.csv"
df = pd.read_csv(file_path)
df.info()

table_id = f"{project_id}.deb_bootcamp.addresses"
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("order_cost", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("shipping_cost", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("order_total", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("tracking_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("shipping_service", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("estimated_delivery_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("delivered_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
    ],
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
)

file_path = "data/orders.csv"
df = pd.read_csv(file_path, parse_dates=["created_at",  "estimated_delivery_at", "delivered_at"])
df.info()

table_id = f"{project_id}.deb_bootcamp.orders"
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("quantity", bigquery.SqlTypeNames.INTEGER),
    ],
)
file_path = "data/order_items.csv"
df = pd.read_csv(file_path)
df.info()

table_id = f"{project_id}.deb_bootcamp.order_items"
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("event_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("session_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("event_type", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("page_url", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
    ],
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
)
file_path = "data/events.csv"
df = pd.read_csv(file_path, parse_dates=["created_at"])
df.info()

table_id = f"{project_id}.deb_bootcamp.events"
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")