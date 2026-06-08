import os
import time
import json
import boto3
import mysql.connector
import pandas as pd
import requests
from datetime import datetime

# MinIO (Data Lake) Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET_NAME = "rivus-data"

# MySQL (rivus-bi) Configuration
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "metatron")

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
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        
        # Example: Extracting metadata or actual table references from Metatron.
        # You will need to change 'your_target_table' to the actual Metatron table you want to sync.
        query = "SHOW TABLES;" 
        df = pd.read_sql(query, conn)
        
        # If you have a specific table for datasets, e.g., 'dataset' or 'datasource':
        # df = pd.read_sql("SELECT * FROM datasource", conn)

        conn.close()
        return df
    except Exception as e:
        print(f"Error extracting from MySQL: {e}")
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

def notify_rivus_ai():
    print(f"[{datetime.now()}] Notifying rivus-ai to sync new data from the Data Lake...")
    try:
        # Example POST request to rivus-ai to tell it new data is available in MinIO
        payload = {
            "data_lake_url": f"http://{MINIO_ENDPOINT}/{BUCKET_NAME}/rivus_bi_export.csv",
            "source": "rivus-bi"
        }
        # Uncomment this in production when RIVUS_AI_URL is valid
        # response = requests.post(RIVUS_AI_URL, json=payload)
        # print(f"rivus-ai response: {response.status_code}")
        print("Notification sent successfully (simulated).")
    except Exception as e:
        print(f"Error notifying rivus-ai: {e}")

def main():
    print("Starting real Data Lake synchronization service...")
    while True:
        # 1. Extract data from rivus-bi (Metatron MariaDB)
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
