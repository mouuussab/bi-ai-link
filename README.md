# BI-AI Link Data Lake

This repository contains the data lake synchronization layer that acts as a real-time bridge between **Data Formulator (rivus-ai)** and **Metatron (rivus-bi)**.

## Architecture

The project relies on a message-driven architecture to achieve near real-time synchronization (under 1 minute) between the two platforms.

**Stack:**
- **Python / FastAPI**: Exposes a REST API (`/sync`) to receive data events from either platform.
- **Kafka / Zookeeper**: The message broker acting as the data lake's real-time events backbone.
- **Background Worker**: A Kafka consumer script (`sync_worker.py`) that reads synchronization events from Kafka and handles pushing or applying them to the target platform.

## How It Works

1. When a change happens in **rivus-bi**, a POST request is sent to `http://<sync-api-url>:8000/sync`.
2. The `sync-api` (FastAPI) receives this payload and produces a message to the `data_sync_events` Kafka topic.
3. The `sync-worker` (Kafka Consumer) immediately receives the message, processes it, and updates **rivus-ai** via its respective APIs or databases.

## Getting Started

### Prerequisites
- Docker and Docker Compose

### Running the Services

To spin up the entire data lake bridge (Kafka, Zookeeper, API, Worker), run:

```bash
docker-compose up --build -d
```

### Checking Logs

To view the real-time processing of events by the worker:

```bash
docker-compose logs -f sync-worker
```

### Testing the Synchronization

You can simulate an event coming from `rivus-bi` intended for `rivus-ai` by sending a cURL request:

```bash
curl -X POST "http://localhost:8000/sync" \
     -H "Content-Type: application/json" \
     -d '{
           "source": "rivus-bi",
           "target": "rivus-ai",
           "entity_id": "chart_12345",
           "data": {
             "chart_type": "bar",
             "updated_at": "2026-06-08T22:00:00Z"
           }
         }'
```

You should see the worker process this event and output success in the Docker logs.
