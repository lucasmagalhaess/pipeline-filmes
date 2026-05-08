from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/src')

os.environ["TMDB_API_KEY"] = "684928d9a01fb1ed4240b70649f96c28"
os.environ["MINIO_ENDPOINT"] = "minio:9000"
os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
os.environ["MINIO_SECRET_KEY"] = "minioadmin"
os.environ["MINIO_BUCKET"] = "filmes-raw"
os.environ["GCP_PROJECT_ID"] = "pipeline-filmes"
os.environ["GCP_BUCKET"] = "pipeline-filmes-data-lake"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/src/../terraform/credentials.json"

default_args = {
    'owner': 'lucas',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_filmes',
    default_args=default_args,
    description='Pipeline completo de dados de filmes',
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['filmes', 'gcp', 'tmdb'],
) as dag:

    def extract_task():
        from extract.tmdb_api import extract
        extract()

    def transform_task():
        from transform.spark_transform import transform
        transform()

    def load_postgres_task():
        from load.load_postgres import load
        load()

    def load_gcp_task():
        from load.load_gcp import load
        load()

    t1_extract = PythonOperator(
        task_id='extrair_filmes_tmdb',
        python_callable=extract_task,
    )

    t2_transform = PythonOperator(
        task_id='transformar_com_spark',
        python_callable=transform_task,
    )

    t3_load_postgres = PythonOperator(
        task_id='carregar_postgresql',
        python_callable=load_postgres_task,
    )

    t4_load_gcp = PythonOperator(
        task_id='carregar_bigquery_gcs',
        python_callable=load_gcp_task,
    )

    t1_extract >> t2_transform >> [t3_load_postgres, t4_load_gcp]
