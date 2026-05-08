import requests
import json
import os
from datetime import datetime
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

def get_popular_movies(page=1):
    url = f"https://api.themoviedb.org/3/movie/popular"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "pt-BR",
        "page": page
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def save_to_minio(data, filename):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    json_data = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    from io import BytesIO
    client.put_object(
        MINIO_BUCKET,
        filename,
        BytesIO(json_data),
        length=len(json_data),
        content_type="application/json"
    )
    print(f"✅ Arquivo {filename} salvo no MinIO!")

def extract():
    print("🎬 Iniciando extração de filmes do TMDB...")
    today = datetime.now().strftime("%Y-%m-%d")
    all_movies = []

    for page in range(1, 6):
        print(f"📄 Buscando página {page}...")
        data = get_popular_movies(page)
        all_movies.extend(data["results"])

    payload = {
        "extraction_date": today,
        "total_movies": len(all_movies),
        "movies": all_movies
    }

    filename = f"raw/movies/{today}/popular_movies.json"
    save_to_minio(payload, filename)
    print(f"🎉 Extração concluída! {len(all_movies)} filmes salvos!")
    return payload

if __name__ == "__main__":
    extract()
