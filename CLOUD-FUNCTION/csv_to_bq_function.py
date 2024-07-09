import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import functions_framework
from io import BytesIO

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def process_csv_files(cloud_event):
    bucket_name = ""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blobs = bucket.list_blobs()
    csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]

    project_id = ""
    dataset_id = ""
    table_name = ""

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_name)

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV

    for blob in csv_blobs:
        file_content = blob.download_as_bytes()
        df = pd.read_csv(BytesIO(file_content))
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="%d-%b-%y %H:%M:%S.%f")
        df["Timestamp"] = df["Timestamp"].astype(str)

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        
        blob.delete()
        
        print(f"{blob.name} uploaded to {table_name}")