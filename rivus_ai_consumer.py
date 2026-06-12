import os
import threading
import json
import time
from datetime import datetime
from pathlib import Path

import boto3
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer

app = FastAPI(title="rivus-ai-consumer")
# Allow the Rivus AI UI (or other frontends) to call these endpoints
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET_NAME = os.getenv("BUCKET_NAME", "rivus-data")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "datalake.updates")

IMPORT_DIR = Path(os.getenv("IMPORT_DIR", "/app/imports"))
IMPORT_DIR.mkdir(parents=True, exist_ok=True)

# MinIO S3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=boto3.session.Config(signature_version='s3v4')
)

last_processed = {"file": None, "timestamp": None}


def download_from_minio(bucket: str, key: str) -> str:
    """Download object from MinIO and save locally. Returns local path."""
    local_path = IMPORT_DIR / Path(key).name
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        body = resp['Body'].read()
        with open(local_path, 'wb') as f:
            f.write(body)
        return str(local_path)
    except Exception as e:
        raise


def process_message(payload: dict):
    """Process a datalake update payload: download and perform placeholder processing.
    Returns local file path on success, or raises on error.
    """
    bucket = payload.get('bucket', BUCKET_NAME)
    key = payload.get('key')
    if not key:
        raise ValueError(f"Invalid payload, missing 'key': {payload}")

    action = payload.get('action', 'import')
    local_path = IMPORT_DIR / Path(key).name

    print(f"[{datetime.now()}] Processing update for {bucket}/{key} with action {action}")

    if action == 'delete':
        if local_path.exists():
            local_path.unlink()
            print(f"Deleted local file {local_path}")
        return str(local_path)

    local_file = download_from_minio(bucket, key)

    # Record last processed
    last_processed['file'] = local_file
    last_processed['timestamp'] = datetime.now().isoformat()
    print(f"Downloaded to {local_file}")
    return local_file


def kafka_consumer_loop():
    """Background thread that consumes Kafka messages and processes them."""
    print(f"Starting Kafka consumer connected to {KAFKA_BOOTSTRAP_SERVERS}, topic {KAFKA_TOPIC}")
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000
            )

            for message in consumer:
                try:
                    payload = message.value
                    print(f"Received Kafka message: {payload}")
                    process_message(payload)
                except Exception as e:
                    print(f"Error processing Kafka message: {e}")

            consumer.close()
        except Exception as e:
            print(f"Kafka consumer error: {e}")
            time.sleep(5)


@app.on_event("startup")
def start_consumer_thread():
    # Ensure Kafka topic exists before starting consumer
    try:
        from kafka_setup import ensure_topic
        ensure_topic(KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
    except Exception as e:
        print(f"Warning: could not ensure Kafka topic at startup: {e}")

    thread = threading.Thread(target=kafka_consumer_loop, daemon=True, name="kafka-consumer")
    thread.start()


@app.get("/health")
def health():
    return {"status": "ok", "last_processed": last_processed}


@app.post("/import")
def manual_import(payload: dict):
    """Manual import endpoint. Provide JSON with 'bucket' and 'key'.
    Downloads the object, attempts to parse CSV/JSON, and returns parsed rows for the UI.
    """
    bucket = payload.get('bucket', BUCKET_NAME)
    key = payload.get('key')
    if not key:
        raise HTTPException(status_code=400, detail="Missing 'key' in payload")
    try:
        local_file = process_message({'bucket': bucket, 'key': key})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {e}")

    # Try to parse the downloaded file as CSV or JSON
    try:
        # Infer by extension
        if local_file.endswith('.json'):
            import json as pyjson
            with open(local_file, 'r', encoding='utf-8') as f:
                data = pyjson.load(f)
            if isinstance(data, list):
                rows = data
            else:
                rows = [data]
        else:
            # CSV fallback using pandas for robustness
            import pandas as pd
            df = pd.read_csv(local_file)
            rows = df.to_dict(orient='records')
    except Exception as e:
        # Parsing failed; still return the file path so UI can fetch manually if desired
        print(f"Failed to parse downloaded file {local_file}: {e}")
        return {"status": "imported", "file": local_file, "rows": []}

    return {"status": "imported", "file": local_file, "rows": rows}


@app.post("/delete")
def manual_delete(payload: dict):
    """Manual delete endpoint. Removes the file from local imports."""
    bucket = payload.get('bucket', BUCKET_NAME)
    key = payload.get('key')
    if not key:
        raise HTTPException(status_code=400, detail="Missing 'key' in payload")
    try:
        process_message({'bucket': bucket, 'key': key, 'action': 'delete'})
        return {"status": "deleted", "key": key}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {e}")


@app.get("/last")
def get_last():
    return last_processed


@app.get("/datasets")
def list_datasets():
    """List objects in the configured MinIO bucket so the Rivus AI UI can show available datasets."""
    try:
        resp = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        items = []
        for o in resp.get("Contents", []):
            # LastModified may be a datetime
            lm = o.get("LastModified")
            lm_s = lm.isoformat() if hasattr(lm, "isoformat") else str(lm)
            items.append({
                "key": o.get("Key"),
                "size": o.get("Size"),
                "last_modified": lm_s
            })
        return {"bucket": BUCKET_NAME, "objects": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("rivus_ai_consumer:app", host="0.0.0.0", port=8000, log_level="info")
