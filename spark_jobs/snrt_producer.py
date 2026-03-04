import psycopg2, json, logging
from kafka import KafkaProducer
from datetime import datetime, date
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("snrt_producer")

def serialize(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

producer = KafkaProducer(
    bootstrap_servers='forja_kafka:9092',
    value_serializer=lambda v: json.dumps(v, default=serialize).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    linger_ms=10,
    batch_size=16384
)

conn = psycopg2.connect(
    host="169.255.179.24", port=5432,
    dbname="snrt_stats", user="snrt_readonly", password="6AOd3Dm2"
)
conn.autocommit = True  # évite le blocage transaction
cur = conn.cursor()

TABLES = {
    "watchings":              "snrt-watchings",
    "users":                  "snrt-users",
    "profiles":               "snrt-profiles",
    "contents":               "snrt-contents",
    "subscriptions":          "snrt-subscriptions",
    "user_subscriptions":     "snrt-user-subscriptions",
    "programs":               "snrt-programs",
    "category_category":      "snrt-categories",
    "user_fav":               "snrt-user-fav",
    "user_like":              "snrt-user-like",
}

for table, topic in TABLES.items():
    try:
        cur.execute(f"SELECT * FROM {table} LIMIT 50000;")
        cols = [d[0] for d in cur.description]
        count = 0
        for row in cur.fetchall():
            record = dict(zip(cols, row))
            key = record.get('id', count)
            producer.send(topic, key=key, value=record)
            count += 1
        producer.flush()
        logger.info(f"✅ {table} → {topic} : {count} rows")
    except Exception as e:
        logger.error(f"❌ {table} : {e}")

conn.close()
logger.info("🎯 SNRT Producer terminé.")
