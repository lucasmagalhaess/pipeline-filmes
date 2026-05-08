import json
import os
import psycopg2
from minio import Minio
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "filmes-raw")

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_CONFIG = {
    "host": DB_HOST,
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def read_from_minio(filename):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    response = client.get_object(MINIO_BUCKET, filename)
    return json.loads(response.read().decode("utf-8"))

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movies_staging (
            movie_id INTEGER PRIMARY KEY,
            title VARCHAR(500),
            original_title VARCHAR(500),
            description TEXT,
            release_date VARCHAR(20),
            rating FLOAT,
            vote_count INTEGER,
            popularity FLOAT,
            original_language VARCHAR(10),
            extraction_date VARCHAR(20),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    print("✅ Tabela movies_staging criada!")

def load_movies(movies, conn):
    cursor = conn.cursor()
    inserted = 0
    for movie in movies:
        cursor.execute("""
            INSERT INTO movies_staging (
                movie_id, title, original_title, description,
                release_date, rating, vote_count, popularity,
                original_language, extraction_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (movie_id) DO UPDATE SET
                rating = EXCLUDED.rating,
                vote_count = EXCLUDED.vote_count,
                popularity = EXCLUDED.popularity,
                extraction_date = EXCLUDED.extraction_date
        """, (
            movie["movie_id"],
            movie["title"],
            movie["original_title"],
            movie["description"],
            movie["release_date"],
            movie["rating"],
            movie["vote_count"],
            movie["popularity"],
            movie["original_language"],
            movie["extraction_date"]
        ))
        inserted += 1
    conn.commit()
    return inserted

def load():
    print("🐘 Iniciando carga no PostgreSQL...")
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"processed/movies/{today}/popular_movies_processed.json"

    print("📥 Lendo dados processados do MinIO...")
    data = read_from_minio(filename)
    movies = data["movies"]

    print(f"🔌 Conectando ao PostgreSQL em {DB_HOST}...")
    conn = psycopg2.connect(**DB_CONFIG)

    create_table(conn)

    print(f"📥 Inserindo {len(movies)} filmes...")
    inserted = load_movies(movies, conn)

    conn.close()
    print(f"🎉 {inserted} filmes carregados no PostgreSQL!")
    return inserted

if __name__ == "__main__":
    load()
