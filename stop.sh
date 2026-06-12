#!/bin/bash
echo "========================================"
echo "🛑 Stopping BI-AI Link Services..."
echo "========================================"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

echo "📦 Bringing down Data Lake and Sync Worker..."
docker compose down

echo "✅ BI-AI Link services stopped successfully."
