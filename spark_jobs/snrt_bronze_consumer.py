import os, time, logging, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, year, month, dayofmonth, lit
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SNRT-BRONZE] %(levelname)s — %(message)s")
log = logging.getLogger("SNRT-BRONZE")

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
BRONZE_BASE    = "s3a://forja-datalake/bronze/snrt"
INTERVAL       = 120
BATCH_SIZE     = 50000
OFFSET_DIR     = "/tmp/snrt_offsets"

TOPICS = {
    "snrt-watchings":          "watchings",
    "snrt-users":              "users",
    "snrt-profiles":           "profiles",
    "snrt-contents":           "contents",
    "snrt-subscriptions":      "subscriptions",
    "snrt-user-subscriptions": "user_subscriptions",
    "snrt-programs":           "programs",
    "snrt-categories":         "categories",
    "snrt-user-fav":           "user_fav",
    "snrt-user-like":          "user_like",
}

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Bronze-SNRT-Consumer")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint",                  MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",                MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",                MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access",         "true")
        .config("spark.hadoop.fs.s3a.impl",                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",  "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions",                   "4")
        .getOrCreate())

def load_offset(topic):
    path = f"{OFFSET_DIR}/{topic.replace('.', '_')}.txt"
    try:
        with open(path) as f:
            return int(f.read().strip())
    except:
        return 0

def save_offset(topic, offset):
    os.makedirs(OFFSET_DIR, exist_ok=True)
    path = f"{OFFSET_DIR}/{topic.replace('.', '_')}.txt"
    with open(path, "w") as f:
        f.write(str(offset))

def run_batch(spark, topic, table_name, starting_offset):
    df = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())

    count = df.count()
    if count == 0:
        return starting_offset

    max_offset = df.selectExpr("max(offset)").collect()[0][0]
    log.info(f"📥 [{table_name}] {count} messages (offset {starting_offset} → {max_offset})")

    df_bronze = (df
        .selectExpr(
            "CAST(value AS STRING) as raw_json",
            "CAST(key AS STRING)   as kafka_key",
            "timestamp             as kafka_timestamp",
            "partition             as kafka_partition",
            "offset                as kafka_offset"
        )
        .withColumn("ingested_at", current_timestamp())
        .withColumn("table_name",  lit(table_name))
        .withColumn("year",        year(col("ingested_at")))
        .withColumn("month",       month(col("ingested_at")))
        .withColumn("day",         dayofmonth(col("ingested_at")))
    )

    output_path = f"{BRONZE_BASE}/{table_name}"
    (df_bronze.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(output_path))

    log.info(f"✅ [{table_name}] → {output_path} — offset suivant: {max_offset + 1}")
    return max_offset + 1

def main():
    log.info("🚀 Démarrage FORJA Bronze Consumer (SNRT) — mode batch périodique")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    offsets = {topic: load_offset(topic) for topic in TOPICS}
    log.info(f"📌 Offsets initiaux: {offsets}")
    while True:
        for topic, table_name in TOPICS.items():
            try:
                new_offset = run_batch(spark, topic, table_name, offsets[topic])
                if new_offset != offsets[topic]:
                    offsets[topic] = new_offset
                    save_offset(topic, new_offset)
            except Exception as e:
                log.error(f"❌ [{table_name}] Erreur: {e}")
        log.info(f"⏳ Prochain batch dans {INTERVAL}s...")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
