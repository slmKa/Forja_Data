import os
import json
import time
import logging
from datetime import datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension
from google.oauth2 import service_account
from kafka import KafkaProducer

# ── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GA4-PRODUCER] %(levelname)s — %(message)s"
)
log = logging.getLogger("GA4-PRODUCER")

# ── Config ───────────────────────────────────────────────────
PROPERTY_ID     = os.getenv("GA4_PROPERTY_ID", "XXXXXXXXX")  # ← à remplacer
CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/app/credentials.json")
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "ga4.events")
INTERVAL_SEC    = int(os.getenv("PRODUCER_INTERVAL", "300"))  # toutes les 5 min

# ── Rapports à collecter ─────────────────────────────────────
REPORTS = {
    "page_views": {
        "dimensions": ["pagePath", "pageTitle"],
        "metrics":    ["screenPageViews", "activeUsers", "averageSessionDuration"]
    },
    "user_traffic": {
        "dimensions": ["sessionSource", "sessionMedium"],
        "metrics":    ["sessions", "newUsers", "bounceRate"]
    },
    "device_info": {
        "dimensions": ["deviceCategory", "operatingSystem", "browser"],
        "metrics":    ["activeUsers", "sessions"]
    },
    "events": {
        "dimensions": ["eventName"],
        "metrics":    ["eventCount", "eventCountPerUser"]
    },
    "content_engagement": {
        "dimensions": ["pagePath"],
        "metrics":    ["userEngagementDuration", "engagedSessions", "scrolledUsers"]
    }
}

# ── Authentification GA4 ─────────────────────────────────────
def init_ga4_client():
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)

# ── Kafka Producer ───────────────────────────────────────────
def init_kafka():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

# ── Lecture rapport GA4 ──────────────────────────────────────
def fetch_report(client, report_name, config):
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name=d) for d in config["dimensions"]],
        metrics=[Metric(name=m) for m in config["metrics"]],
        date_ranges=[DateRange(start_date="1daysAgo", end_date="today")]
    )
    response = client.run_report(request)
    rows = []
    for row in response.rows:
        record = {
            "report_type": report_name,
            "timestamp": datetime.utcnow().isoformat(),
        }
        for i, dim in enumerate(config["dimensions"]):
            record[dim] = row.dimension_values[i].value
        for i, met in enumerate(config["metrics"]):
            raw = row.metric_values[i].value
            try:
                record[met] = float(raw) if '.' in raw else int(raw)
            except (ValueError, TypeError):
                record[met] = None
        rows.append(record)
    return rows

# ── Main loop ────────────────────────────────────────────────
def main():
    log.info("🚀 Démarrage du GA4 Producer pour FORJA...")
    ga4    = init_ga4_client()
    log.info("✅ Client GA4 connecté")
    kafka  = init_kafka()
    log.info(f"✅ Kafka connecté sur {KAFKA_BROKER}")

    cycle = 0
    while True:
        cycle += 1
        total = 0
        log.info(f"\n{'='*50}")
        log.info(f"🔄 Cycle #{cycle} — {datetime.utcnow()} UTC")

        for name, config in REPORTS.items():
            log.info(f"📊 Lecture rapport: {name}")
            try:
                rows = fetch_report(ga4, name, config)
                for row in rows:
                    kafka.send(KAFKA_TOPIC, value=row)
                    total += 1
                log.info(f"   ✅ {len(rows)} lignes envoyées")
            except Exception as e:
                log.error(f"   ❌ Erreur rapport {name}: {e}")

        kafka.flush()
        log.info(f"\n✅ Cycle #{cycle} terminé — {total} messages envoyés dans '{KAFKA_TOPIC}'")
        log.info(f"⏳ Prochain cycle dans {INTERVAL_SEC}s...")
        time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    main()
