import os
import time
import json
import boto3
import mysql.connector
from pydruid.db import sqlalchemy as druid_db
import pandas as pd
import requests
from datetime import datetime

# MinIO (Data Lake) Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET_NAME = "rivus-data"

# rivus-bi Data Source Configuration
SOURCE_TYPE = os.getenv("SOURCE_TYPE", "druid") # 'mysql' or 'druid'

# MySQL Configuration
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "metatron")

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

def extract_from_rivus_bi():
    print(f"[{datetime.now()}] Connecting to rivus-bi MariaDB...")
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
        
        query = "SHOW TABLES;" 
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"MySQL connection failed: {e}")
        print(">>> SIMULATION MODE: Generating fake rivus-bi data instead.")
        
        # Create a mock Pandas DataFrame to simulate rivus-bi data
        mock_data = {
            "id": [1, 2, 3],
            "dataset_name": ["sales_q1", "user_growth", "revenue_2026"],
            "status": ["active", "active", "processing"],
            "last_updated": [datetime.now().isoformat()] * 3
        }
        return pd.DataFrame(mock_data)

def extract_from_druid():
    print(f"[{datetime.now()}] Connecting to rivus-bi Apache Druid...")
    try:
        if DRUID_HOST == "localhost" or not DRUID_HOST:
            raise Exception("No real Druid host configured. Falling back to simulation.")
            
        # Connect to Druid's SQL endpoint
        engine = druid_db.create_engine(f"druid://{DRUID_HOST}:{DRUID_PORT}/druid/v2/sql/")
        
        # Query the analytics data from Druid
        query = f"SELECT * FROM {DRUID_DATASOURCE} LIMIT 1000"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"Druid connection failed: {e}")
        print(">>> SIMULATION MODE: Generating fake Druid analytics data instead.")
        
        mock_data = {
            "timestamp": [datetime.now().isoformat()] * 3,
            "region": ["US", "EU", "APAC"],
            "revenue": [15000.50, 12000.00, 9500.75]
        }
        return pd.DataFrame(mock_data)

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

def notify_rivus_ai():
    print(f"[{datetime.now()}] Notifying rivus-ai to sync new data from the Data Lake...")
    try:
        # Example POST request to rivus-ai to tell it new data is available in MinIO
        payload = {
            "data_lake_url": f"http://{MINIO_ENDPOINT}/{BUCKET_NAME}/rivus_bi_export.csv",
            "source": "rivus-bi"
        }
        print(f"Sending payload to {RIVUS_AI_URL}: {payload}")
        # Send the actual POST request
        response = requests.post(RIVUS_AI_URL, json=payload, timeout=5)
        print(f"rivus-ai response: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to reach rivus-ai at {RIVUS_AI_URL}. Simulation continuing.")
        print(f"Error details: {e}")
    except Exception as e:
        print(f"Error notifying rivus-ai: {e}")

def main():
    print("Starting real Data Lake synchronization service...")
    while True:
        # 1. Extract data from rivus-bi (Metatron MySQL or Druid)
        if SOURCE_TYPE == "druid":
            df = extract_from_druid()
        else:
            df = extract_from_rivus_bi()
        
        # 2. Load the data into the MinIO Data Lake
        success = load_to_minio_data_lake(df)
        
        # 3. If new data was uploaded, notify rivus-ai
        if success:
            notify_rivus_ai()
            
        # Wait 1 minute before checking again
        time.sleep(60)

if __name__ == "__main__":
    # Wait for MinIO to initialize
    time.sleep(10)
    main()
