#imports
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import xmltodict
import json

#GCS connection information
project_id='cric-stats-analyzer'
bucket_name='cric-stats-bucket'
source_folder='raw_data'
destination_folder='processed_data'
dataset_id='cricsheet_data'
table_name='odi_data'

def gcs_import():
    try:
        #Storage Connections
        client=storage.Client(project=project_id)
        bucket=client.bucket(bucket_name)
        blobs=bucket.list_blobs(prefix=source_folder)
        #BigQuery Connections
        bq_client  = bigquery.Client(project = project_id)
        bq_dataset  = bq_client.dataset(dataset_id)
        try:
            table_ref = bq_dataset.table(table_name)
            print(table_ref)
        except NotFound:
            print('Table Not Found')
        
        #batch counter
        count=0
        batch_data=[]
        for blob in blobs:
            print(blob.name)
            #should the files be remaned as processed_?  and not blob.name.startswith("raw_data/processed_")):      #batch size max
            if(blob.name.endswith(".xml")):
                if(count<1000):
                    jsonl=xml_to_jsonl(client, bucket, blob)
                    bucket.rename_blob(blob, new_name=blob.name.replace('raw_data/', 'updated_data/'))
                    if(jsonl!=0):
                        batch_data.append(jsonl)
                    print(batch_data)
                    count+=1
                else:
                    #load job config
                    print("load the job")
                    bq_load_job(bq_client,batch_data,table_ref)
                    count=0
                    batch_data=[]

        if len(batch_data)!=0:
            print("load job")
            bq_load_job(bq_client,batch_data,table_ref)

    except Exception as e:
        print("GCS IMPORT ERROR:",e)

def xml_to_jsonl(client, bucket, blob):
    data_new={}
    try:
        try:
            with blob.open('r') as xml_file:
                data_dict = xmltodict.parse(xml_file.read())
                data_dict=data_dict["cricsheet"]

            if 'info' in data_dict and 'innings' in data_dict and \
            'match_type_number' in data_dict['info'] and 'match_type' in data_dict['info'] and \
            'lineups' in data_dict['info'] and 'lineup' in data_dict['info']['lineups'] and \
                'inning' in data_dict['innings']:
                deliveries=[]
                for inning in data_dict['innings']['inning']:
                    deliveries+=inning['deliveries']['delivery']
                data_new={
                'match_type':data_dict['info']['match_type'],
                'match_type_number':data_dict['info']['match_type_number'],
                'players':data_dict['info']['lineups']['lineup'][0]['players']['player']+data_dict['info']['lineups']['lineup'][0]['players']['player'],
                'deliveries':deliveries
                }
                print("NEW JSON:",data_new)

                file_name=(blob.name.split('/')[1]).split('.')[0]
                object_name=f'{destination_folder}/{file_name}.jsonl'
                print(object_name)
                #creating new blob in processed folder
                blob_new = bucket.blob(object_name)
                with blob_new.open('w') as jsonl_file:
                    jsonl_file.write(json.dumps(data_new))
                print("Blob new:",blob_new)
                return data_new
            else:
                print("Data check not passed. Schema doesn't match requirements")
                return 0
        except Exception as e:
            print("CANT OPEN FILE! error:",e)

    except Exception as e:
        print("XML TO JSON CONVERSION ERROR:",e)

def bq_load_job(client,batch_data,table):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        #schema=table_schema,
        autodetect=True,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    load_job = client.load_table_from_json(batch_data, table, job_config = job_config)
    load_job.result()

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

#set task for each folder
#TASKS
gcs_import_task=PythonOperator(
    task_id='gcs_import_task',
    python_callable=gcs_import,
    dag=test_dag,
)

#task dependencies
gcs_import_task