# BI-AI Link Data Lake

This repository contains the **Data Lake synchronization layer** that acts as a real-time bridge between **Metatron (rivus-bi)** and **Data Formulator (rivus-ai)**.

## The Data Lake Architecture

Because **rivus-bi** does not have a native option to export data, this project sets up a real Data Lake using **MinIO** (an S3-compatible Object Storage system) to act as the centralized hub.

**How it works (Real Integration, No Simulation):**
1. **Extraction**: A Python background worker (`sync_worker.py`) directly connects to rivus-bi's underlying metadata database (MariaDB/MySQL). It queries the database every 60 seconds to detect any new data or modifications that came from other resources or user actions.
2. **Data Lake Storage**: The worker pulls this data into a Pandas DataFrame and physically exports it as a CSV (or JSON/Parquet) file directly into the **MinIO Data Lake** bucket (`rivus-data`).
3. **Notification/Sync**: Once the file is physically secured in the Data Lake, the worker sends an API request to **rivus-ai**, passing the MinIO URL of the new data. Rivus-ai then ingests this file instantly. 

This ensures that within 1 minute, any new data entering rivus-bi is physically mirrored in the Data Lake and available in rivus-ai automatically.

## Getting Started

### Prerequisites
- Docker and Docker Compose
- The actual connection strings to your `rivus-bi` MariaDB/MySQL server.
- The actual API endpoint for `rivus-ai` to trigger a refresh.

### Setup

Open `docker-compose.yml` and modify the environment variables under `sync-worker` to match your actual servers:

```yaml
      - MYSQL_HOST=your_rivus_bi_mysql_host
      - MYSQL_PORT=3306
      - MYSQL_USER=root
      - MYSQL_PASSWORD=root_password
      - MYSQL_DATABASE=metatron
      - RIVUS_AI_URL=http://your_rivus_ai_host:port/api/refresh
```

*Note: You must also modify the SQL query inside `sync_worker.py` (around line 38) to select the exact tables (e.g., `datasource` or `dataset`) you want to extract from Metatron.*

### Running the Services

To spin up the Data Lake (MinIO) and the Synchronization Worker:

```bash
docker-compose up --build -d
```

### Accessing the Data Lake

You can manually inspect the Data Lake and see the files being transferred from rivus-bi:
- **MinIO Console**: `http://localhost:9001`
- **Username**: `admin`
- **Password**: `password123`

### Checking Logs

To ensure the worker is successfully extracting from MySQL and uploading to MinIO:

```bash
docker-compose logs -f sync-worker
```
