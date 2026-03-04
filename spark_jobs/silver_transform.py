import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, lower, round as spark_round,
    to_timestamp, current_timestamp, lit,
    regexp_replace, coalesce
)
from pyspark.sql.types import DoubleType, IntegerType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [SILVER-TRANSFORM] %(levelname)s — %(message)s'
)
log = logging.getLogger(__name__)

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",        "http://minio:9000")
MINIO_USER      = os.getenv("MINIO_ROOT_USER",        "minioadmin")
MINIO_PASS      = os.getenv("MINIO_ROOT_PASSWORD",    "minioadmin123")

BRONZE_PATH     = "s3a://forja-datalake/bronze/ga4/"
SILVER_PATH     = "s3a://forja-datalake/silver/ga4/"

def create_spark_session():
    log.info("🔧 Création SparkSession Silver...")
    spark = (
        SparkSession.builder
        .appName("FORJA-Silver-GA4-Transform")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",          MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",          MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("✅ SparkSession Silver prête")
    return spark

def read_bronze(spark):
    log.info(f"📖 Lecture Bronze depuis: {BRONZE_PATH}")
    df = spark.read.parquet(BRONZE_PATH)
    log.info(f"   → {df.count()} lignes lues depuis Bronze")
    return df

def clean_common(df):
    log.info("🧹 Nettoyage commun...")
    df_clean = (
        df
        .filter(col("report_type").isNotNull())
        .withColumn("report_type",    trim(lower(col("report_type"))))
        .withColumn("source",         trim(lower(col("source"))))
        .withColumn("country",        trim(col("country")))
        .withColumn("city",           trim(col("city")))
        .withColumn("deviceCategory", trim(lower(col("deviceCategory"))))
        .withColumn("eventName",      trim(lower(col("eventName"))))
        .withColumn("pagePath", when(
            col("pagePath").isin("(not set)", "", "(none)"), None
        ).otherwise(col("pagePath")))
        .withColumn("country", when(
            col("country").isin("(not set)", ""), None
        ).otherwise(col("country")))
        .withColumn("city", when(
            col("city").isin("(not set)", ""), None
        ).otherwise(col("city")))
        .withColumn("eventName", when(
            col("eventName").isin("(not set)", ""), None
        ).otherwise(col("eventName")))
        .withColumn("screenPageViews",
            spark_round(coalesce(col("screenPageViews").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("activeUsers",
            spark_round(coalesce(col("activeUsers").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("sessions",
            spark_round(coalesce(col("sessions").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("newUsers",
            spark_round(coalesce(col("newUsers").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("bounceRate",
            spark_round(coalesce(col("bounceRate").cast(DoubleType()), lit(0.0)), 4))
        .withColumn("averageSessionDuration",
            spark_round(coalesce(col("averageSessionDuration").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("eventCount",
            spark_round(coalesce(col("eventCount").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("transformed_at", current_timestamp())
    )
    return df_clean

def deduplicate(df):
    log.info("🔍 Déduplication...")
    dedup_keys = [
        "report_type", "date",
        "pagePath", "country", "city",
        "deviceCategory", "eventName"
    ]
    df_dedup = df.dropDuplicates(dedup_keys)
    count_before = df.count()
    count_after  = df_dedup.count()
    log.info(f"   → Avant: {count_before} | Après: {count_after} | Supprimées: {count_before - count_after}")
    return df_dedup

def enrich(df):
    log.info("✨ Enrichissement...")
    df_enriched = (
        df
        .withColumn("device_category_fr", when(
            col("deviceCategory") == "mobile", "Mobile"
        ).when(
            col("deviceCategory") == "desktop", "Desktop"
        ).when(
            col("deviceCategory") == "tablet", "Tablette"
        ).otherwise("Autre"))
        .withColumn("is_new_user", when(col("newUsers") > 0, True).otherwise(False))
        .withColumn("bounce_rate_pct",
            spark_round(col("bounceRate") * 100, 2))
        .withColumn("avg_session_duration_min",
            spark_round(col("averageSessionDuration") / 60, 2))
    )
    return df_enriched

def write_silver(df):
    log.info(f"💾 Écriture Silver vers: {SILVER_PATH}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("report_type", "date")
        .parquet(SILVER_PATH)
    )
    log.info("✅ Silver écrit avec succès")

def validate_silver(spark):
    log.info("🔎 Validation Silver...")
    df_check = spark.read.parquet(SILVER_PATH)
    total = df_check.count()
    log.info(f"   📊 Total lignes Silver: {total}")
    log.info("   📋 Répartition par report_type:")
    df_check.groupBy("report_type").count().orderBy("count", ascending=False).show()
    log.info("   🌍 Top 10 pays:")
    df_check.filter(col("country").isNotNull()) \
            .groupBy("country").sum("activeUsers") \
            .orderBy("sum(activeUsers)", ascending=False).show(10)
    log.info("   📱 Répartition appareils:")
    df_check.groupBy("device_category_fr").sum("activeUsers") \
            .orderBy("sum(activeUsers)", ascending=False).show()

def main():
    log.info("🚀 Démarrage FORJA Silver Transform (GA4)...")
    spark       = create_spark_session()
    df_bronze   = read_bronze(spark)
    df_clean    = clean_common(df_bronze)
    df_dedup    = deduplicate(df_clean)
    df_enriched = enrich(df_dedup)
    write_silver(df_enriched)
    validate_silver(spark)
    log.info("🎉 Silver Transform terminé avec succès !")
    spark.stop()

if __name__ == "__main__":
    main()
