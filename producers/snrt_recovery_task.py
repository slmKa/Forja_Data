import psycopg2
import json
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from celery import shared_task
from celery_app import app

# Configuration PostgreSQL snrt_stats
DB_CONFIG = {
    "host": "localhost",       # adapte si différent
    "port": 5432,
    "dbname": "snrt_stats",
    "user": "postgres",        # adapte
    "password": "ton_password" # adapte
}

BRONZE_PATH = os.path.expanduser("~/forja_pipeline/datalake/bronze/snrt_stats")

def get_existing_timestamps():
    """Lit les fichiers bronze déjà présents pour détecter les gaps."""
    existing = set()
    if not os.path.exists(BRONZE_PATH):
        os.makedirs(BRONZE_PATH, exist_ok=True)
        return existing
    for fname in os.listdir(BRONZE_PATH):
        if fname.endswith(".json"):
            # format attendu: snrt_stats_2024-01.json
            parts = fname.replace("snrt_stats_", "").replace(".json", "")
            existing.add(parts)
    return existing

@app.task(bind=True, max_retries=5, default_retry_delay=60)
def recover_month(self, year, month):
    """Récupère les données d'un mois donné depuis snrt_stats."""
    period_key = f"{year:04d}-{month:02d}"
    try:
        start_dt = datetime(year, month, 1)
        end_dt = start_dt + relativedelta(months=1)

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Adapte la table et les colonnes à ton schéma réel
        cur.execute("""
            SELECT * FROM stats
            WHERE created_at >= %s AND created_at < %s
        """, (start_dt, end_dt))

        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        if not rows:
            print(f"[{period_key}] Aucune donnée trouvée.")
            return f"empty:{period_key}"

        records = [dict(zip(colnames, row)) for row in rows]

        # Conversion datetime en string pour JSON
        for r in records:
            for k, v in r.items():
                if isinstance(v, datetime):
                    r[k] = v.isoformat()

        out_file = os.path.join(BRONZE_PATH, f"snrt_stats_{period_key}.json")
        with open(out_file, "w") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        print(f"[{period_key}] {len(records)} lignes sauvegardées -> {out_file}")
        return f"ok:{period_key}:{len(records)}"

    except Exception as exc:
        print(f"[{period_key}] ERREUR: {exc} — retry dans 60s")
        raise self.retry(exc=exc)


def launch_full_recovery():
    """Lance le recovery de Jan 2024 jusqu'au mois courant."""
    existing = get_existing_timestamps()
    start = datetime(2024, 1, 1)
    now = datetime.utcnow()
    current = start
    launched = 0

    while current <= now:
        period_key = f"{current.year:04d}-{current.month:02d}"
        if period_key not in existing:
            recover_month.delay(current.year, current.month)
            print(f"Tâche envoyée pour {period_key}")
            launched += 1
        else:
            print(f"{period_key} déjà présent, skip.")
        current += relativedelta(months=1)

    print(f"\nTotal tâches lancées: {launched}")

if __name__ == "__main__":
    launch_full_recovery()
