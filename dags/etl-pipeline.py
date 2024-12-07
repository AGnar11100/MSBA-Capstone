from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery
import os
import requests
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from connection.secret import project_name, gcs_service_acc, gcs_bucket_raw, gcs_bucket_transform, gcs_cluster_name, my_region, my_dataset_name, local_csvs_loc, tables_names, local_script_loc

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

SERVICE_ACCOUNT = gcs_service_acc
BUCKET_RAW = gcs_bucket_raw
BUCKET_TRANSFORMED = gcs_bucket_transform
CLUSTER_NAME = gcs_cluster_name
REGION = my_region
PROJECT_ID = project_name
DATASET_NAME = my_dataset_name
TABLE_NAMES = tables_names  # This now includes the list from your secrets.py file
LOCAL_CSVS = local_csvs_loc  # List of multiple CSVs from secrets.py
SCRIPT_LOCATION = local_script_loc


def create_dataset():
    hook = BigQueryHook()
    client = hook.get_client()
    dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION
    client.create_dataset(dataset, exists_ok=True)

def create_table(table_name):
    hook = BigQueryHook()
    client = hook.get_client()
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table_name.replace(' ', '_')}"

    schema = []
    if table_name == 'Zips Counties':
        schema = [
            bigquery.SchemaField("ZIP", "INTEGER"),
            bigquery.SchemaField("County", "STRING"),
            bigquery.SchemaField("Area_Code", "STRING"),
        ]
    elif table_name == 'WA EV Population':
        schema = [
            bigquery.SchemaField("County", "STRING"),
            bigquery.SchemaField("City", "STRING"),
            bigquery.SchemaField("State", "STRING"),
            bigquery.SchemaField("ZIP", "INTEGER"),
            bigquery.SchemaField("Model_Year", "STRING"),
            bigquery.SchemaField("Make", "STRING"),
            bigquery.SchemaField("Model", "STRING"),
            bigquery.SchemaField("Electric_Vehicle_Type", "STRING"),
            bigquery.SchemaField("CAFV_Eligibility", "STRING"),
            bigquery.SchemaField("Electric_Range", "FLOAT"),
            bigquery.SchemaField("Legislative_District", "FLOAT"),
            bigquery.SchemaField("DOL_Vehicle_ID", "INTEGER"),
            bigquery.SchemaField("Electric_Utility", "STRING"),
            bigquery.SchemaField("Census_Tract", "FLOAT"),
            bigquery.SchemaField("Longitude", "FLOAT"),
            bigquery.SchemaField("Latitude", "FLOAT"),
        ]
    elif table_name == 'WA EV History':
        schema = [
            bigquery.SchemaField("Date", "STRING"),
            bigquery.SchemaField("PHEV_Count", "INTEGER"),
            bigquery.SchemaField("BEV_Count", "INTEGER"),
            bigquery.SchemaField("EV_Total", "INTEGER"),
        ]
    elif table_name == 'Charging Stations':
        schema = [
            bigquery.SchemaField("Fuel_Type_Code", "STRING"),
            bigquery.SchemaField("Station_Name", "STRING"),
            bigquery.SchemaField("City", "STRING"),
            bigquery.SchemaField("State", "STRING"),
            bigquery.SchemaField("ZIP", "INTEGER"),
            bigquery.SchemaField("Status_Code", "STRING"),
            bigquery.SchemaField("Groups_With_Access_Code", "STRING"),
            bigquery.SchemaField("Access_Days_Time", "STRING"),
            bigquery.SchemaField("EV_Network", "STRING"),
            bigquery.SchemaField("Geocode_Status", "STRING"),
            bigquery.SchemaField("Latitude", "FLOAT"),
            bigquery.SchemaField("Longitude", "FLOAT"),
            bigquery.SchemaField("Date_Last_Confirmed", "STRING"),
            bigquery.SchemaField("ID", "INTEGER"),
            bigquery.SchemaField("Updated_At", "STRING"),
            bigquery.SchemaField("Open_Date", "STRING"),
            bigquery.SchemaField("EV_Connector_Types", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("Groups_With_Access_Code_French", "STRING"),
            bigquery.SchemaField("Access_Code", "STRING"),
            bigquery.SchemaField("EV_Workplace_Charging", "BOOLEAN"),
        ]
    elif table_name == 'Power Plants':
        schema = [
            bigquery.SchemaField("X", "FLOAT"),
            bigquery.SchemaField("Y", "FLOAT"),
            bigquery.SchemaField("OBJECTID", "INTEGER"),
            bigquery.SchemaField("Plant_Code", "INTEGER"),
            bigquery.SchemaField("Plant_Name", "STRING"),
            bigquery.SchemaField("Utility_ID", "INTEGER"),
            bigquery.SchemaField("Utility_Name", "STRING"),
            bigquery.SchemaField("sector_name", "STRING"),
            bigquery.SchemaField("City", "STRING"),
            bigquery.SchemaField("County", "STRING"),
            bigquery.SchemaField("State", "STRING"),
            bigquery.SchemaField("ZIP", "FLOAT"),
            bigquery.SchemaField("PrimSource", "STRING"),
            bigquery.SchemaField("source_desc", "STRING"),
            bigquery.SchemaField("tech_desc", "STRING"),
            bigquery.SchemaField("Install_MW", "FLOAT"),
            bigquery.SchemaField("Total_MW", "FLOAT"),
            bigquery.SchemaField("Longitude", "FLOAT"),
            bigquery.SchemaField("Latitude", "FLOAT"),
        ]

    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

with DAG(
    dag_id='EV_Data_Pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['current project'],
) as dag:

    create_raw_bucket = GCSCreateBucketOperator(
        task_id='create_raw_bucket',
        bucket_name=BUCKET_RAW,
    )

    create_transformed_bucket = GCSCreateBucketOperator(
        task_id='create_transformed_bucket',
        bucket_name=BUCKET_TRANSFORMED,
    )

    upload_local_csvs_to_raw = [
    LocalFilesystemToGCSOperator(
        task_id=f'upload_local_csv_{i}_to_raw',
        src=local_csv,
        dst=os.path.basename(local_csv),  # Use the original filename as the destination in GCS
        bucket=BUCKET_RAW,
    ) for i, local_csv in enumerate(LOCAL_CSVS)
    ]

    upload_script_to_transformed = LocalFilesystemToGCSOperator(
        task_id='upload_script_to_transformed',
        src=SCRIPT_LOCATION,
        dst='transform.py',
        bucket=BUCKET_TRANSFORMED,
    )

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config={
            'gce_cluster_config': {
                'service_account': SERVICE_ACCOUNT,
                'internal_ip_only': True,
            },
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-2',
                'disk_config': {'boot_disk_size_gb': 60}
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-2',
                'disk_config': {'boot_disk_size_gb': 60}
            },
        },
    )

    pyspark_job = {
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': f'gs://{BUCKET_TRANSFORMED}/transform.py',
            'args': [
                f'gs://{BUCKET_RAW}/',
                f'gs://{BUCKET_TRANSFORMED}/transformed_data/'
            ],
        },
    }

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_bigquery_dataset = PythonOperator(
        task_id='create_bigquery_dataset',
        python_callable=create_dataset,
    )

    create_bigquery_tables = [
        PythonOperator(
            task_id=f'create_bigquery_table_{i}',
            python_callable=create_table,
            op_args=[table_name],
        ) for i, table_name in enumerate(TABLE_NAMES)
    ]

    load_to_bigquery = [
    GCSToBigQueryOperator(
        task_id=f'load_{table_name.replace(" ", "_")}_to_bigquery',
        bucket=BUCKET_TRANSFORMED,
        source_objects=[f'transformed_data/{table_name.lower().replace(" ", "_")}.csv/*.csv'],
        destination_project_dataset_table=f'{PROJECT_ID}:{DATASET_NAME}.{table_name.replace(" ", "_")}',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        skip_leading_rows=1,  # Assumes that your CSV files contain headers
    ) for table_name in ['Zips_Counties', 'Charging_Stations', 'Power_Plants', 'WA_EV_Population', 'WA_EV_History']
]


    

    # Define task dependencies
create_raw_bucket >> upload_local_csvs_to_raw >> create_dataproc_cluster
create_transformed_bucket >> upload_script_to_transformed >> create_dataproc_cluster

create_dataproc_cluster >> submit_pyspark_job >> delete_dataproc_cluster

delete_dataproc_cluster >> create_bigquery_dataset >> create_bigquery_tables

# Dynamically chain creation of BigQuery tables to their respective data loading tasks
for table_task, load_task in zip(create_bigquery_tables, load_to_bigquery):
    table_task >> load_task
