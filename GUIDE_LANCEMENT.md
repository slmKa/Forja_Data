# GUIDE DE LANCEMENT PIPELINE FORJA - DE A Z
# Projet PFE - SNRT - Leila Boukhie
# Derniere mise a jour : 27/02/2026
# ============================================================

## 0. CONNEXION AU SERVEUR
    ssh leilaboukhiepfe@vmi2110033
    cd ~/forja_pipeline


## 1. VERIFIER L'ETAT DES CONTENEURS
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

    OK : tous les conteneurs affichent "Up"
    Si certains manquent → passer a l'etape 2


## 2. LANCER L'INFRASTRUCTURE (ordre important)
    docker compose up -d zookeeper kafka schema-registry kafka-ui postgres minio
    sleep 25
    docker logs forja_kafka 2>&1 | grep -E "started|ERROR" | tail -5

    OK : "[KafkaServer id=1] started" sans erreur Zookeeper


## 3. VERIFIER LE RESEAU (si probleme de connectivite)
    docker network inspect forja_net --format '{{range .Containers}}{{.Name}} {{end}}'

    OK : tous les conteneurs apparaissent dans forja_net
    Si un conteneur manque :
    docker network connect forja_net <nom_conteneur>

    Conteneurs attendus :
    forja_zookeeper, forja_kafka, forja_schema_registry, forja_kafka_ui,
    forja_minio, forja_spark_master, forja_spark_worker,
    ga4-producer, forja_postgres


## 4. LANCER SPARK
    docker compose up -d spark-master spark-worker
    sleep 10
    docker logs forja_spark_master --tail 5

    OK : pas d'erreur critique
    UI Spark : http://<IP>:8080


## 5. LANCER LE GA4 PRODUCER
    docker restart ga4-producer
    sleep 10 && docker logs ga4-producer --tail 15

    OK : "Sent batch report_type=content_engagement"
    KO : "NoBrokersAvailable" → verifier etape 3 (reseau)


## 6. LANCER LE JOB BRONZE SPARK (Kafka → MinIO)
    mkdir -p ~/forja_pipeline/logs

    nohup docker exec forja_spark_master spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.2.0 \
      /opt/spark_jobs/bronze_consumer.py > ~/forja_pipeline/logs/bronze.log 2>&1 &

    Attendre 45s puis verifier :
    sleep 45 && tail -20 ~/forja_pipeline/logs/bronze.log | grep -v "NativeCode\|MetricsConfig\|WARN"

    OK : "Stream actif" ou "Batch 0 committed"
    KO : "NoSuchBucket" → verifier MinIO (etape 2)
    KO : "ClassNotFoundException" → verifier packages spark-submit


## 7. VERIFIER LES FICHIERS PARQUET DANS MINIO
    sleep 75 && docker exec forja_minio mc ls --recursive local/forja-datalake/bronze/ga4/ 2>&1 | tail -10

    OK : fichiers .parquet partitionnes par annee/mois/jour
    KO : dossier vide → attendre encore 60s et relancer la commande


## 8. MONITORING EN TEMPS REEL (3 terminaux pour la demo)

    -- Terminal 1 : Logs Spark live --
    tail -f ~/forja_pipeline/logs/bronze.log | grep -v "NativeCode\|MetricsConfig"

    -- Terminal 2 : Fichiers MinIO toutes les 30s --
    watch -n 30 "docker exec forja_minio mc ls --recursive local/forja-datalake/bronze/ga4/ 2>&1 | tail -10"

    -- Terminal 3 : Navigateur --
    Kafka UI     → http://<IP>:8082           (topics, messages ga4.events)
    Spark UI     → http://<IP>:8080           (jobs actifs)
    MinIO        → http://<IP>:9001           minioadmin / minioadmin123
    PgAdmin      → http://<IP>:5050
    Appsmith     → http://<IP>:80


## 9. ARRET PROPRE
    # Arreter le job Spark Bronze
    kill $(pgrep -f bronze_consumer) 2>/dev/null || true

    # Arreter les conteneurs forja (sans toucher pgadmin/appsmith)
    docker compose stop


## 10. CHECKLIST RAPIDE AVANT DEMO
    [ ] ssh → cd ~/forja_pipeline
    [ ] docker compose up -d                          (etape 2)
    [ ] Kafka started                                 (etape 2)
    [ ] Reseau forja_net OK                           (etape 3)
    [ ] Spark master + worker up                      (etape 4)
    [ ] ga4-producer envoie sans erreur               (etape 5)
    [ ] bronze_consumer.py lance en arriere-plan      (etape 6)
    [ ] Fichiers .parquet dans MinIO                  (etape 7)
    [ ] 3 interfaces ouvertes dans le navigateur      (etape 8)


## 11. ERREURS COURANTES ET SOLUTIONS

    ERREUR : NoBrokersAvailable (ga4-producer)
    CAUSE   : ga4-producer pas sur le meme reseau que kafka
    FIX     : docker network connect forja_net ga4-producer
              docker restart ga4-producer

    ERREUR : Unable to canonicalize address zookeeper:2181
    CAUSE   : Kafka et Zookeeper sur des reseaux differents
    FIX     : docker rm -f forja_kafka
              docker compose up -d kafka

    ERREUR : no configuration file provided: not found
    CAUSE   : tu n'es pas dans ~/forja_pipeline
    FIX     : cd ~/forja_pipeline

    ERREUR : data may have been lost (Spark WARN)
    CAUSE   : Kafka a ete redemarre, offsets reinitialises
    FIX     : normal apres restart, le stream continue quand meme

    ERREUR : NoSuchBucket (Spark)
    CAUSE   : bucket forja-datalake pas cree dans MinIO
    FIX     : docker exec forja_minio mc mb local/forja-datalake

    ERREUR : endpoint already exists in network
    CAUSE   : conteneur deja connecte au reseau
    FIX     : pas une erreur, ignorer


## 12. ARCHITECTURE PIPELINE (rappel)

    GA4 (Google Analytics)
        |
        v
    ga4-producer (Docker)
        |  topic : ga4.events
        v
    Kafka + Zookeeper + Schema Registry
        |
        v
    Spark Streaming (bronze_consumer.py)
        |  format : Parquet, partitionne par date
        v
    MinIO - Data Lake Bronze
    (local/forja-datalake/bronze/ga4/)

    Pipeline : GA4 → Kafka → Spark → MinIO (Bronze Layer)

# ============================================================
# FIN DU GUIDE
# ============================================================

