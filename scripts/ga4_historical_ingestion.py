import os
import time
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
import pandas as pd
from io import BytesIO
import boto3
from botocore.client import Config

# ─── CONFIG ───────────────────────────────────────────────────────────────────
PROPERTY_ID = "442462711"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = "bronze"
PREFIX = "ga4/historical"
CHUNK_DAYS = 30
START_DATE = "2024-05-01"
END_DATE = "2026-02-28"

DIMENSIONS = ["date", "pagePath", "deviceCategory", "country", "sessionSource", "sessionMedium"]
METRICS = ["sessions", "activeUsers", "newUsers", "screenPageViews", "bounceRate", "averageSessionDuration", "eventCount"]

# ─── MINIO CLIENT ─────────────────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

def upload_parquet(df, key):
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"  ✅ Uploaded: {key} ({len(df)} rows, {buf.tell()/1024:.1f} KB)")

def fetch_ga4_chunk(client, start, end):
    dims = [Dimension(name=d) for d in DIMENSIONS]
    mets = [Metric(name=m) for m in METRICS]
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=dims,
        metrics=mets,
        date_ranges=[DateRange(start_date=start, end_date=end)],
        limit=100000,
    )
    response = client.run_report(request)
    rows = []
    for row in response.rows:
        r = {DIMENSIONS[i]: row.dimension_values[i].value for i in range(len(DIMENSIONS))}
        r.update({METRICS[i]: row.metric_values[i].value for i in range(len(METRICS))})
        rows.append(r)
    return pd.DataFrame(rows)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
client = BetaAnalyticsDataClient()
current = datetime.strptime(START_DATE, "%Y-%m-%d")
end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
chunk_num = 0
total_rows = 0
total_size = 0

print(f"🚀 Starting GA4 historical ingestion: {START_DATE} → {END_DATE}")
while current <= end_dt:
    chunk_end = min(current + timedelta(days=CHUNK_DAYS - 1), end_dt)
    start_str = current.strftime("%Y-%m-%d")
    end_str = chunk_end.strftime("%Y-%m-%d")
    print(f"\n📦 Chunk {chunk_num+1}: {start_str} → {end_str}")
    try:
        df = fetch_ga4_chunk(client, start_str, end_str)
        if df.empty:
            print("  ⚠️  No data for this period, skipping.")
        else:
            key = f"{PREFIX}/{start_str}_{end_str}/data.parquet"
            upload_parquet(df, key)
            total_rows += len(df)
            chunk_num += 1
    except Exception as e:
        print(f"  ❌ Error: {e}")
    time.sleep(1)  # avoid API rate limit
    current = chunk_end + timedelta(days=1)

print(f"\n🎉 Done! {chunk_num} chunks, {total_rows} total rows ingested.")
