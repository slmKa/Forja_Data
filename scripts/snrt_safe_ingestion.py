import os, time, psycopg2, pandas as pd, boto3
from datetime import datetime, timedelta
from io import BytesIO
from botocore.client import Config

DB_CONFIG = {
    "host": os.getenv("SNRT_DB_HOST", "localhost"),
    "port": os.getenv("SNRT_DB_PORT", "5432"),
    "dbname": "snrt_stats",
    "user": os.getenv("SNRT_DB_USER", "snrt_readonly"),
    "password": os.getenv("SNRT_DB_PASSWORD", ""),
}
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY","minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY","minioadmin"),
    config=Config(signature_version="s3v4"))

TABLES = ["watchings", "contents", "users"]  # adapte selon tes tables

def is_access_window():
    """Retourne True si on est dans la fenêtre d'accès 00:00-15:00"""
    now = datetime.now()
    return now.hour < 15

def wait_for_access_window():
    """Attend minuit si on est hors fenêtre"""
    now = datetime.now()
    if now.hour >= 15:
        midnight = (now + timedelta(days=1)).replace(hour=0, minute=5, second=0)
        wait_sec = (midnight - now).seconds
        print(f"⏳ Hors fenêtre d'accès. Attente jusqu'à minuit ({wait_sec//3600}h{(wait_sec%3600)//60}m)...")
        time.sleep(wait_sec)

def ingest_table(table, date_str):
    conn = psycopg2.connect(**DB_CONFIG)
    query = f"SELECT * FROM {table} WHERE DATE(created_at) = '{date_str}'"
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        print(f"  ⚠️  {table}: no data for {date_str}")
        return
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    key = f"snrt_stats/{table}/{date_str}/data.parquet"
    s3.put_object(Bucket="bronze", Key=key, Body=buf.getvalue())
    print(f"  ✅ {table}: {len(df)} rows → {key}")

target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
print(f"🚀 SNRT ingestion for: {target_date}")

for table in TABLES:
    if not is_access_window():
        wait_for_access_window()
    try:
        ingest_table(table, target_date)
    except Exception as e:
        print(f"  ❌ {table}: {e}")

print("✅ SNRT ingestion done.")
