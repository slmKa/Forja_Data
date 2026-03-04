#!/bin/bash
echo "===== $(date) ====="
echo "--- Containers ---"
docker ps --format "{{.Names}}: {{.Status}}"
echo "--- Kafka Offset (ga4.events) ---"
docker exec forja_kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic ga4.events --time -1 2>/dev/null
echo "--- Bronze Consumer Offset ---"
docker exec forja_spark_master bash -c "cat /tmp/bronze_offset.txt 2>/dev/null || echo 'ABSENT'"
echo "--- Derniers fichiers MinIO ---"
docker exec forja_minio bash -c "mc alias set local http://localhost:9000 minioadmin minioadmin123 2>/dev/null && mc ls local/forja-datalake/bronze/ga4/ --recursive | sort | tail -5"
