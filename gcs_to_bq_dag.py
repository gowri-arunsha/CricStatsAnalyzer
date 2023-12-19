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
#import os


#GCS connection information
project_id='cric-stats-analyzer'
bucket_name='cric-stats-bucket'
source_folder='raw_data'
destination_folder='processed_data'
dataset_id='cricsheet_data'
table_name='test_data'

def gcs_import():
    try:
        #Storage Connections
        client=storage.Client(project=project_id)
        bucket=client.bucket(bucket_name)
        blobs=bucket.list_blobs(prefix=source_folder)
        #BigQuery connections
        bq_client  = bigquery.Client(project = project_id)
        try:    
            bq_dataset  = bq_client.dataset(dataset_id)
            table_ref = dataset.table(table_name)
            #?
            print(client.get_table(table_ref))
        except NotFound:
            print('Table Not Found')

        print(bq_dataset)
        #table = dataset.table(table_id)

        #batch counter
        count=0
        batch_data=[]
        for blob in blobs:
            print(blob.name)
            if(blob.name.endswith(".xml") and not blob.name.startswith("raw_data/processed_")):      #batch size max
                #batch processing files
                if(count<1000):
                    jsonl=xml_to_jsonl(client, bucket, blob)
                    if(jsonl!=0):
                        batch_data.append(jsonl)
                    print(batch_data)
                    count+=1
                else:
                    #load job config
                    print("load the job")
                    count=0
                    batch_data=[]

                ##REMOVE THIS COMMENT
                #bucket.rename_blob(blob, new_name=blob.name.replace('raw_data/', 'raw_data/processed_'))
        if len(batch_data)!=0:
            print("load job")

                #use extends() ?

    except Exception as e:
        print("GCS IMPORT ERROR:",e)

# #WORKING FUNCTION
# def bq_upload(batch):
#     client=storage.Client(project_id)
#     bucket=client.bucket(bucket_name)
#     blobs=bucket.list_blobs(prefix=destination_folder)
#     count=0
#     for blob in blobs:
#         print(blob.name)


def xml_to_jsonl(client, bucket, blob):
    data_new={}
    try:
        try:
            with blob.open('r') as xml_file:
                data_dict = xmltodict.parse(xml_file.read())
                data_dict=data_dict["cricsheet"]

            #data_new={}
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
                    #\n required?
                print("Blob new:",blob_new)
                #remove this file from raw folder
                return data_new

            else:
                print("Data check not passed. Schema doesn't match requirements")
                return 0
        except Exception as e:
            print("CANT OPEN FILE! error:",e)
            
        #Write JSON objects as lines in the JSONL file

        # with blob_new.open('w') as jsonl_file:
        #     jsonl_file.write(json.dumps(data_new) + '\n')
        #     print("Blob new:",blob_new)


        # with blob_new.open('w') as jsonl_file:
        #     root_key = list(data_dict.keys())[0] if data_dict else None
        #     if root_key and isinstance(data_dict[root_key], list):
        #         for item in data_dict[root_key]:
        #             jsonl_file.write(json.dumps(item) + '\n')
        #     else:
        #         jsonl_file.write(json.dumps(data_dict) + '\n')
        #     print("Blob new:",blob_new)





    except Exception as e:
        print("XML TO JSON CONVERSION ERROR:",e)
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

# bq_upload_task=PythonOperator(
#     task_id='bq_upload_task',
#     python_callable=bq_upload,
#     op_args=[1000],
#     dag=test_dag,
# )


# upload_gcs_to_bq_task = GCSToBigQueryOperator(
#     task_id='upload_gcs_to_bq_task',
#     bucket=bucket_name,
#     source_objects=['processed_data/693431.jsonl'], #only .jsonl files
#     destination_project_dataset_table=f'{project_id}.cricsheet_data.test_table',
#     schema_fields=None,  # Autodetect schema
#     source_format='NEWLINE_DELIMITED_JSON',
#     create_disposition='CREATE_IF_NEEDED',  # Options: 'CREATE_NEVER', 'CREATE_IF_NEEDED'
#     write_disposition='WRITE_APPEND',  # Options: 'WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY'
#     autodetect=True,
#     dag=test_dag,
# )

#task dependencies
gcs_import_task
#>>bq_upload_task
#>>upload_gcs_to_bq_task
