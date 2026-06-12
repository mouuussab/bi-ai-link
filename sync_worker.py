import os
import time
import json
import boto3
import mysql.connector
from pydruid.db import sqlalchemy as druid_db
import pandas as pd
import requests
from datetime import datetime
from kafka import KafkaProducer

# MinIO (Data Lake) Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET_NAME = "rivus-data"

# rivus-bi Data Source Configuration
SOURCE_TYPE = os.getenv("SOURCE_TYPE", "druid") # 'mysql' or 'druid'

# Which Rivus BI dataset/table to export (defaults to the dataset you mentioned)
TARGET_DATASET = os.getenv("TARGET_DATASET", "ohlcv_test_data")

# MySQL Configuration
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "rivus_bi")

# Druid Configuration
DRUID_HOST = os.getenv("DRUID_HOST", "localhost")
DRUID_PORT = int(os.getenv("DRUID_PORT", 8888))
DRUID_DATASOURCE = os.getenv("DRUID_DATASOURCE", "your_druid_datasource")

# rivus-ai Configuration
RIVUS_AI_URL = os.getenv("RIVUS_AI_URL", "http://localhost/api/refresh")

s3_client = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=boto3.session.Config(signature_version='s3v4')
)

# Kafka producer (Redpanda) initialization
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"Kafka producer initialized to {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    print(f"Failed to initialize Kafka producer: {e}")
    producer = None

def extract_from_rivus_bi():
    """Extract target dataset from Rivus BI's MySQL metadata DB.
    Falls back to simulation if DB isn't reachable.
    Returns a tuple (df, filename) where filename is the object name to store in MinIO.
    """
    print(f"[{datetime.now()}] Connecting to rivus-bi MariaDB to read dataset '{TARGET_DATASET}'...")
    filename = f"{TARGET_DATASET}.csv"
    try:
        # If no host is provided or if we explicitly want to simulate
        if MYSQL_HOST == "your_rivus_bi_mysql_host" or not MYSQL_HOST:
            raise Exception("No real MySQL host configured. Falling back to simulation.")

        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )

        # Attempt to select the target dataset/table. If it doesn't exist this will raise and fall back to simulation.
        query = f"SELECT * FROM {TARGET_DATASET} LIMIT 100000"
        df = pd.read_sql(query, conn)
        conn.close()
        return df, filename
    except Exception as e:
        print(f"MySQL read failed for dataset '{TARGET_DATASET}': {e}")
        print(">>> SIMULATION MODE: Generating fake rivus-bi data instead.")

        # Create a mock Pandas DataFrame to simulate rivus-bi data
        mock_data = {
            "timestamp": [datetime.now().isoformat()] * 3,
            "open": [100, 101, 102],
            "high": [110, 111, 112],
            "low": [90, 91, 92],
            "close": [105, 106, 107],
            "volume": [1000, 1100, 1200]
        }
        df = pd.DataFrame(mock_data)
        return df, filename

def get_rivus_bi_datasources():
    print(f"[{datetime.now()}] Fetching list of datasources from Apache Druid (UI agnostic)...")
    try:
        import requests
        query = "SELECT datasource, MAX(version) as version FROM sys.segments WHERE is_published = 1 GROUP BY 1"
        url = f"http://{DRUID_HOST}:{DRUID_PORT}/druid/v2/sql/"
        response = requests.post(url, json={"query": query}, timeout=15)
        response.raise_for_status()
        data = response.json()
        return {row['datasource']: row['version'] for row in data}
    except Exception as e:
        print(f"Failed to fetch datasources from Druid: {e}")
        return {}

def extract_from_druid(datasource):
    print(f"[{datetime.now()}] Extracting {datasource} from Apache Druid...")
    try:
        import requests
        query = f"SELECT * FROM {datasource} LIMIT 100000"
        url = f"http://{DRUID_HOST}:{DRUID_PORT}/druid/v2/sql/"
        response = requests.post(url, json={"query": query}, timeout=15)
        response.raise_for_status()
        data = response.json()
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Extraction failed for {datasource}: {e}")
        return None

def load_to_minio_data_lake(df, filename="rivus_bi_export.csv"):
    if df is None or df.empty:
        print("No data to upload to MinIO.")
        return False
        
    print(f"[{datetime.now()}] Uploading data to MinIO Data Lake ({BUCKET_NAME}/{filename})...")
    csv_data = df.to_csv(index=False)
    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=csv_data,
            ContentType='text/csv'
        )
        print("Successfully uploaded to MinIO.")
        return True
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")
        return False

def notify_rivus_ai(filename="rivus_bi_export.csv", action="import"):
    print(f"[{datetime.now()}] Notifying rivus-ai to {action} data: {filename}...")
    try:
        url = RIVUS_AI_URL if action == "import" else RIVUS_AI_URL.replace("/import", "/delete")
        payload = {
            "data_lake_url": f"http://{MINIO_ENDPOINT}/{BUCKET_NAME}/{filename}",
            "source": "rivus-bi",
            "bucket": BUCKET_NAME,
            "key": filename,
            "action": action
        }
        print(f"Sending payload to {url}: {payload}")
        response = requests.post(url, json=payload, timeout=5)
        print(f"rivus-ai response: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to reach rivus-ai at {url}. Simulation continuing.")
        print(f"Error details: {e}")
    except Exception as e:
        print(f"Error notifying rivus-ai: {e}")


def notify_kafka(filename="rivus_bi_export.csv", action="import"):
    print(f"[{datetime.now()}] Sending Kafka notification ({action}): {filename}...")
    if producer is None:
        print("Kafka producer not initialized; skipping Kafka notification.")
        return
    try:
        payload = {
            "bucket": BUCKET_NAME,
            "key": filename,
            "timestamp": datetime.now().isoformat(),
            "source": "rivus-bi",
            "action": action
        }
        producer.send("datalake.updates", payload)
        producer.flush()
        print("Kafka notification sent.")
    except Exception as e:
        print(f"Failed to send Kafka notification: {e}")

last_sync_state = {}

def main():
    global last_sync_state
    print("Starting real Data Lake synchronization service...")
    while True:
        if SOURCE_TYPE == "druid":
            datasources = get_rivus_bi_datasources()
            
            # 1. Extract and upload all active datasources if they changed
            for ds, version in datasources.items():
                if last_sync_state.get(ds) != version:
                    df = extract_from_druid(ds)
                    if df is not None and not df.empty:
                        filename = f"{ds}.csv"
                        if load_to_minio_data_lake(df, filename=filename):
                            last_sync_state[ds] = version
            
            # 2. Cleanup deleted datasources from MinIO
            try:
                objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME).get('Contents', [])
                valid_filenames = {f"{ds}.csv" for ds in datasources.keys()}
                for obj in objects:
                    key = obj['Key']
                    if key.endswith('.csv') and key not in valid_filenames:
                        print(f"[{datetime.now()}] Deleting {key} from Data Lake as it was removed from Rivus BI.")
                        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
                        ds_name = key[:-4]
                        if ds_name in last_sync_state:
                            del last_sync_state[ds_name]
            except Exception as e:
                print(f"Cleanup failed: {e}")
        else:
            df, filename = extract_from_rivus_bi()
            load_to_minio_data_lake(df, filename=filename)
            
        # Wait 3 seconds before checking again for near-instant synchronization
        time.sleep(3)

if __name__ == "__main__":
    # Wait for MinIO to initialize
    time.sleep(10)
    main()
