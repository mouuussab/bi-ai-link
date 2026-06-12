#!/bin/bash

echo "========================================"
echo "🚀 Starting BI-AI Ecosystem..."
echo "========================================"

# 1. Start BI-AI Link (Data Lake & Sync Worker)
echo "📦 Starting Data Lake and Sync Worker..."
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"
docker compose up --build -d

# No need to bridge Metatron to the network since it's running natively on the host

echo "✅ Background services are running!"
echo "   - MinIO Data Lake: http://localhost:9001"
echo "   - To view sync logs: docker compose logs -f sync-worker"
echo ""


