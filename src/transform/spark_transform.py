import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, to_date, round
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

def read_from_minio(filename):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    response = client.get_object(MINIO_BUCKET, filename)
    data = json.loads(response.read().decode("utf-8"))
    return data

def save_to_minio(data, filename):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    json_data = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        MINIO_BUCKET,
        filename,
        BytesIO(json_data),
        length=len(json_data),
        content_type="application/json"
    )
    print(f"✅ Arquivo {filename} salvo no MinIO!")

def transform():
    print("⚡ Iniciando transformação com Spark...")

    spark = SparkSession.builder \
        .appName("pipeline-filmes") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"raw/movies/{today}/popular_movies.json"

    print("📥 Lendo dados do MinIO...")
    raw_data = read_from_minio(filename)
    movies = raw_data["movies"]

    df = spark.createDataFrame(movies)

    print("🔄 Transformando dados...")
    df_transformed = df.select(
        col("id").cast("integer").alias("movie_id"),
        col("title"),
        col("original_title"),
        col("overview").alias("description"),
        col("release_date"),
        col("vote_average").alias("rating"),
        col("vote_count"),
        col("popularity"),
        col("original_language"),
        lit(today).alias("extraction_date")
    ).filter(
        col("vote_count") > 100
    ).orderBy(
        col("rating").desc()
    )

    print(f"📊 Total de filmes após transformação: {df_transformed.count()}")
    df_transformed.show(5, truncate=False)

    result = [row.asDict() for row in df_transformed.collect()]
    output_filename = f"processed/movies/{today}/popular_movies_processed.json"
    save_to_minio({"movies": result, "total": len(result)}, output_filename)

    spark.stop()
    print("🎉 Transformação concluída!")
    return result

if __name__ == "__main__":
    transform()
