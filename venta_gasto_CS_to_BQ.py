import functions_framework
import google.cloud.storage as storage
import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import bigquery
from io import BytesIO

# CONSTANTES DE CLOUD STORAGE
BUCKET_NAME = 'ds_venta_gasto'

# CONSTANTES DE BIGQUERY
PROJECT_ID = "psa-fin-general" 
DATASET_ID = "ds_venta_gasto_qas"  # CAMBIAR A PRD TRAS PRUEBAS: "Dataset_Venta_gasto"

# DICCIONARIO ARCHIVO:DATASET
DIC_FILE = {
    "Z1-Ventas actualizable COPA.parquet": "VENTAS_COPA",
    "Z1-Ventas actualizable S4.parquet": "VENTAS_S4",
    "Z1-Ventas actualizable SD.parquet": "VENTAS_SD",
    "Z1-Ventas actualizable TERCEROS.parquet": "VENTAS_TERCEROS",
    "1C-V-Canal distribucion.parquet":"CANAL_DIST",
    "1C-V-Centro.parquet":"CENTRO",
    "1C-V-Gpo articulo Mat equivalente.parquet":"GPO_ART_MAT_EQ",
    "1C-V-Material.parquet":"MATERIAL",
    "1C-V-Mercado.parquet":"MERCADO",
    "1C-V-Organizacion de ventas.parquet":"ORG_VENTAS",
    "1C-V-Sociedad.parquet":"SOCIEDAD",
    "1C-V-Solicitante.parquet":"SOLICITANTE",
    "2B-G-Gastos.parquet":"GASTOS",
    "2C-G-Ceco Cefin.parquet":"CECO_CEFIN",
    "2C-G-Ceco.parquet":"CECO",
    "2C-G-ClCo Cefin.parquet":"CLCO_CEFIN",
    "2C-G-ClCo.parquet":"CLCO",
    "2C-G-InfoObjeto para CeClCo.parquet":"INFOOBJETO_CECLCO",
    "2C-G-Sociedad FI Gasto.parquet":"SOCIEDAD_FI_GASTO",
    "Gastos Ajuste.parquet":"GASTOS_AJUSTES"
}

@functions_framework.cloud_event
def parquet_CS_to_BQ(cloud_event):
    def list_blobs(bucket_name):
        """Lista todos los blobs en el bucket. Es decir, todos los archivos (no hay carpetas)"""
        list_blobs = []
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(bucket_name)
        for blob in blobs:
            list_blobs.append(blob.name)
        return list_blobs

    def rename_columns_parquet(bucket_name, file_name):
        """Renombra las columnas de un archivo Parquet en Cloud Storage"""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Descargar archivo Parquet a memoria
        parquet_data = blob.download_as_bytes()
        table = pq.read_table(pa.BufferReader(parquet_data))

        # Renombrar columnas
        new_column_names = [col.replace('.', '_') for col in table.schema.names]
        table = table.rename_columns(new_column_names)

        # Convertir tabla de nuevo a formato Parquet en memoria
        output_stream = BytesIO()
        pq.write_table(table, output_stream)
        modified_parquet_data = output_stream.getvalue()

        # Reemplazar archivo Parquet modificado en Cloud Storage
        blob.upload_from_string(modified_parquet_data, content_type='application/octet-stream')

        return f"gs://{bucket_name}/{file_name}"

    def CS_to_BQ(file_name, table_id_name):
        """Carga tablas a BigQuery a partir de archivos Parquet"""
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id_name}"
        gcs_uri = rename_columns_parquet(BUCKET_NAME, file_name)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            use_avro_logical_types=True  # Enable character map V2
        )
        # Cargar de GC a BQ
        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        # Esperar a que el trabajo se haga
        load_job.result()

    def main():
        list_files = list_blobs(BUCKET_NAME)
        for file in list_files:
            if file in DIC_FILE:
                try:
                    print(f"Procesando {file} ...")
                    CS_to_BQ(file, DIC_FILE[file])
                    print(f"{file} cargado exitosamente en {DIC_FILE[file]}.")
                except Exception as e:
                    print(f"Error al cargar {file}: {e}")
            else:
                print(f"Archivo {file} no encontrado en el diccionario.")

    main()