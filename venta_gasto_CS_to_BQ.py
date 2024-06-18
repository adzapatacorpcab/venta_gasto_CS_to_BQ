import json
import google.cloud.storage as storage
import pandas as pd
from google.cloud import bigquery

#CONSTANTES DE CLOUD STORAGE
BUCKET_NAME = 'raw_ssff_mx_qa'

#CONSTANTES DE BIGQUERY
PROJECT_ID = "psa-fin-general" 
DATASET_ID = "ds_venta_gasto_qas" # CAMBIAR A PRD TRAS PRUEBAS: "Dataset_Venta_gasto"

#DICCIONARIO ARCHIVO:DATASET

def list_blobs(bucket_name):
    """Lista todos los blobs en el bucket. Es decir, todos los archivos (no hay carpetas)"""
    list_blobs = []
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        list_blobs.append(blob.name)
    return list_blobs

def CS_to_BQ(file_name,table_id_name):
    """Carga tablas a BigQuery a partir de archivos Parquet"""
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id_name}"
    gcs_uri = f"gs://{BUCKET_NAME}/{file_name}.parquet"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    # Cargar de GC a BQ
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    # Esperar al que trabajo se haga
    load_job.result()

def main():