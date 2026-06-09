#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Pulling images (may take a while)..."
docker compose pull || true

echo "Starting services (in background)..."
docker compose up --build -d

# Wait for Kafka to accept connections
echo "Waiting for Kafka to be ready (up to ~60s)..."
for i in {1..30}; do
  if docker exec datalake-kafka bash -c "/usr/bin/kafka-topics --bootstrap-server kafka:9092 --list" >/dev/null 2>&1; then
    echo "Kafka is responding"
    break
  fi
  echo "  waiting... ($i)"
  sleep 2
done

# Ensure topic exists (idempotent)
echo "Ensuring topic 'datalake.updates' exists..."
docker exec datalake-kafka bash -c "/usr/bin/kafka-topics --bootstrap-server kafka:9092 --create --topic datalake.updates --replication-factor 1 --partitions 1 || true"

echo "Startup complete. Helpful follow-ups:"
echo "  Tail logs: sudo docker compose logs -f sync-worker rivus-ai kafka zookeeper minio"
echo "  Test import (if rivus_bi_export.csv exists in rivus-data):"
echo "    curl -X POST -H 'Content-Type: application/json' http://localhost:8000/import -d '{\"bucket\":\"rivus-data\",\"key\":\"rivus_bi_export.csv\"}'"
echo "  Check consumer health: curl http://localhost:8000/health"

echo "If MinIO upload is needed, use the MinIO console at http://localhost:9001 (admin/password123) or mc/SDKs to upload the file."
