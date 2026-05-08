import json
import os
from minio import Minio
from google.cloud import bigquery, storage
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_BUCKET = os.getenv("GCP_BUCKET")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/pipeline-filmes/terraform/credentials.json"

def read_from_minio(filename):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    response = client.get_object(MINIO_BUCKET, filename)
    return json.loads(response.read().decode("utf-8"))

def upload_to_gcs(data, gcs_filename):
    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET)
    blob = bucket.blob(gcs_filename)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False),
        content_type="application/json"
    )
    print(f"✅ Arquivo enviado para GCS: gs://{GCP_BUCKET}/{gcs_filename}")
    return f"gs://{GCP_BUCKET}/{gcs_filename}"

def load_to_bigquery(movies):
    client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.analytics.movies"

    schema = [
        bigquery.SchemaField("movie_id", "INTEGER"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("original_title", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("release_date", "STRING"),
        bigquery.SchemaField("rating", "FLOAT"),
        bigquery.SchemaField("vote_count", "INTEGER"),
        bigquery.SchemaField("popularity", "FLOAT"),
        bigquery.SchemaField("original_language", "STRING"),
        bigquery.SchemaField("extraction_date", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_json(movies, table_id, job_config=job_config)
    job.result()
    print(f"✅ {len(movies)} filmes carregados no BigQuery!")

def load():
    print("☁️ Iniciando carga no GCP...")
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"processed/movies/{today}/popular_movies_processed.json"

    print("📥 Lendo dados processados do MinIO...")
    data = read_from_minio(filename)
    movies = data["movies"]

    print("📤 Enviando para o GCS...")
    gcs_filename = f"processed/movies/{today}/popular_movies_processed.json"
    upload_to_gcs(data, gcs_filename)

    print("📊 Carregando no BigQuery...")
    load_to_bigquery(movies)

    print("🎉 Carga no GCP concluída!")

if __name__ == "__main__":
    load()
