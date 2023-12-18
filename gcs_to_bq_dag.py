#imports
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from google.cloud import storage
import xmltodict
import json
import os


#GCS connection information
project_id='cric-stats-analyzer'
bucket_name='cric-stats-bucket'
source_folder='raw_data'
destination_folder='processed_data'

####WORKING FUNCTION
def gcs_import():
    try:
        client=storage.Client(project=project_id)
        bucket=client.bucket(bucket_name)
        blobs=bucket.list_blobs(prefix=source_folder)
        print(client,":",bucket)
        for blob in blobs:
            if(blob.name.endswith(".xml")):
                print(blob)
                xml_to_jsonl(client, bucket, blob)
    except Exception as e:
        print("GCS IMPORT ERROR:",e)

def bq_upload():
    client=storage.Client(project_id)
    bucket=client.bucket(bucket_name)
    

def xml_to_jsonl(client, bucket, blob):
    try:
        try:
            with blob.open('r') as xml_file:
                data_dict = xmltodict.parse(xml_file.read())
                data_dict=data_dict["cricsheet"]
            file_name=(blob.name.split('/')[1]).split('.')[0]
            object_name=f'{destination_folder}/{file_name}.jsonl'
            print(object_name)
            #creating new blob in processed folder
            blob_new = bucket.blob(object_name)
            print("Blob new:",blob_new)
        except Exception as e:
            print("CANT OPEN FILE! error:",e)
            
        #Write JSON objects as lines in the JSONL file
        with blob_new.open('w') as jsonl_file:
            root_key = list(data_dict.keys())[0] if data_dict else None
            if root_key and isinstance(data_dict[root_key], list):
                for item in data_dict[root_key]:
                    jsonl_file.write(json.dumps(item) + '\n')
            else:
                jsonl_file.write(json.dumps(data_dict) + '\n')

            print("Blob new:",blob_new)
    except Exception as e:
        print("XML TO JSON ERROR:",e)
    # Read XML file and convert to JSON
    


#DAG default parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
}

#DAG creation
test_dag=DAG(
    'test_dag',
    default_args=default_args,
    description='DAG to upload file to Big Query from a GCS Bucket',
    #schedule=timedelta(days=1),  # Set the schedule interval as needed
    schedule=None,
)

#TASKS
gcs_import_task=PythonOperator(
    task_id='gcs_import_task',
    python_callable=gcs_import,
    dag=test_dag,
)

bq_upload_task=PythonOperator(
    task_id='bq_upload_task',
    python_callable=bq_upload,
    dag=test_dag,
)

#task dependencies
gcs_import_task>>bq_upload_task
