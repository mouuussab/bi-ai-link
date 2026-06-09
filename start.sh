#!/bin/bash

echo "========================================"
echo "🚀 Starting BI-AI Ecosystem..."
echo "========================================"

# 1. Start BI-AI Link (Data Lake & Sync Worker)
echo "📦 Starting Data Lake and Sync Worker..."
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"
docker compose up --build -d

# Wait a moment for docker network to be created
sleep 3

# 2. Connect Metatron to the network
echo "🔗 Bridging Metatron (nifty_bell) to the Data Lake network..."
docker network connect bi-ai-link_default nifty_bell 2>/dev/null || echo "   (Metatron is already bridged)"

echo "✅ Background services are running!"
echo "   - MinIO Data Lake: http://localhost:9001"
echo "   - To view sync logs: docker compose logs -f sync-worker"
echo ""


