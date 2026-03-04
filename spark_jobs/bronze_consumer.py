import os, time, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, year, month, dayofmonth
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BRONZE-CONSUMER] %(levelname)s — %(message)s")
log = logging.getLogger("BRONZE-CONSUMER")

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_GA4       = "ga4.events"
BRONZE_PATH     = "s3a://forja-datalake/bronze/ga4/"
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER      = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS      = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
INTERVAL        = 60
OFFSET_FILE     = "/tmp/bronze_offset.txt"
BATCH_SIZE      = 50000

GA4_SCHEMA = StructType([
    StructField("report_type",             StringType(),  True),
    StructField("date",                    StringType(),  True),
    StructField("pagePath",                StringType(),  True),
    StructField("pageTitle",               StringType(),  True),
    StructField("sessions",                LongType(),    True),
    StructField("screenPageViews",         LongType(),    True),
    StructField("activeUsers",             LongType(),    True),
    StructField("newUsers",                LongType(),    True),
    StructField("bounceRate",              DoubleType(),  True),
    StructField("averageSessionDuration",  DoubleType(),  True),
    StructField("country",                 StringType(),  True),
    StructField("city",                    StringType(),  True),
    StructField("deviceCategory",          StringType(),  True),
    StructField("eventName",               StringType(),  True),
    StructField("eventCount",              LongType(),    True),
    StructField("source",                  StringType(),  True),
    StructField("medium",                  StringType(),  True),
])

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Bronze-GA4-Consumer")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint",                 MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",               MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",               MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access",        "true")
        .config("spark.hadoop.fs.s3a.impl",                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions",                  "4")
        .getOrCreate())

def load_offset():
    try:
        with open(OFFSET_FILE) as f:
            return int(f.read().strip())
    except:
        return 0

def save_offset(offset):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))

def run_batch(spark, starting_offset):
    df = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_GA4)
        .option("startingOffsets", f'{{"ga4.events":{{"0":{starting_offset}}}}}')
        .option("endingOffsets",  f'{{"ga4.events":{{"0":{starting_offset + BATCH_SIZE}}}}}')
        .option("failOnDataLoss", "false")
        .load())

    count = df.count()
    if count == 0:
        log.info("⏳ Aucun nouveau message Kafka")
        return starting_offset

    max_offset = df.selectExpr("max(offset)").collect()[0][0]
    log.info(f"📥 {count} nouveaux messages (offset {starting_offset} → {max_offset})")

    df_bronze = (df
        .selectExpr("CAST(value AS STRING) as json_str",
                    "CAST(key AS STRING) as kafka_key",
                    "timestamp as kafka_timestamp",
                    "partition as kafka_partition",
                    "offset as kafka_offset")
        .withColumn("data",        from_json(col("json_str"), GA4_SCHEMA))
        .withColumn("ingested_at", current_timestamp())
        .select(col("kafka_key"), col("kafka_timestamp"),
                col("kafka_partition"), col("kafka_offset"),
                col("ingested_at"), col("data.*"))
        .withColumn("year",  year(col("ingested_at")))
        .withColumn("month", month(col("ingested_at")))
        .withColumn("day",   dayofmonth(col("ingested_at")))
        )

    from pyspark.sql.functions import coalesce, lit
    df_clean = df_bronze.withColumn(
        "report_type",
        coalesce(df_bronze["report_type"], lit("unknown"))
    )

    (df_clean.write
        .mode("append")
        .partitionBy("year", "month", "day", "report_type")
        .parquet(BRONZE_PATH))

    log.info(f"✅ Bronze écrit dans MinIO — offset suivant: {max_offset + 1}")
    return max_offset + 1

def main():
    log.info("🚀 Démarrage FORJA Bronze Consumer (GA4) — mode batch périodique")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    offset = load_offset()
    log.info(f"📌 Reprise depuis offset: {offset}")
    while True:
        try:
            offset = run_batch(spark, offset)
            save_offset(offset)
        except Exception as e:
            log.error(f"❌ Erreur batch: {e}")
        log.info(f"⏳ Prochain batch dans {INTERVAL}s...")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
