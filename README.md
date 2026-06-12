# BI-AI Link Data Lake

This repository contains the **Data Lake synchronization layer** that acts as a real-time, automated bridge between **Rivus BI Discovery (rivus-bi)** and **Data Formulator (rivus-ai)**. 

Because Rivus BI does not have a native option to stream its internal data directly to external AI tools, this project sets up a real **Data Lake** using **MinIO** to act as a centralized hub and a background worker to constantly mirror data.

## 🚀 How it Works (Start to Finish)

The entire synchronization process is fully automated and requires **Zero Configuration** when adding new data. 

1. **User Action in Rivus BI (rivus-bi)**
   Users upload raw files, connect to external databases, or use "Data Preparation" to clean data. As long as the final data is saved/ingested as a **Data Source** in Rivus BI, it enters Rivus BI's fast-engine (Apache Druid).

2. **Dynamic Extraction (`sync-worker`)**
   A Python background service runs continuously. Every 60 seconds, it queries Rivus BI's Druid engine via REST API to ask for a list of *all* currently available Data Sources. 
   For every Data Source it finds, it extracts the data into a Pandas DataFrame.

3. **Data Lake Storage (MinIO)**
   The worker immediately uploads this extracted data as a physical CSV file into the **MinIO Data Lake** bucket (`rivus-data`). If the data has been modified in Rivus BI, the CSV file in the Data Lake is silently overwritten with the newest version.

4. **Automatic Cleanup (Deletion Syncing)**
   The worker also compares the files inside the Data Lake against the active Data Sources in Rivus BI. If a user deletes a Data Source inside Rivus BI, the worker will detect it's missing and automatically delete the corresponding CSV file from the Data Lake.

5. **AI Ingestion (rivus-ai)**
   Inside **Rivus AI**, users use the "Load from URL" feature (pointing to `http://localhost:9000/rivus-data/<dataset_name>.csv`) and enable **auto-refresh**. Whenever the worker updates the Data Lake file in the background, Rivus AI seamlessly auto-refreshes the charts.

---

## 🛠️ The Technology Stack (Used Tools)

Here is a breakdown of the tools used in this ecosystem and their purpose:

* **Apache Druid (inside Rivus BI)**: This is the high-performance analytics database where Rivus BI stores all of its "Data Sources". Our worker hooks directly into its SQL REST API to extract the active data.
* **MinIO**: A high-performance, S3-compatible object storage server. It serves as our "Data Lake". It is lightweight, scalable, and provides public URLs that Rivus AI can directly stream data from.
* **Python (Sync Worker)**: The brain of the operation. It uses libraries like `requests` (to query Druid), `pandas` (to process tabular data), and `boto3` (to communicate with the MinIO S3 API). 
* **Docker & Docker Compose**: Used to orchestrate the MinIO data lake, the sync worker, and networking (bridging the `datalake` network with the `nifty_bell` bridge network).
* **Kafka & Zookeeper (Redpanda)**: *Scalability layer.* Included in the infrastructure stack to broadcast real-time update events (e.g., "new data available") to multiple downstream consumers or agents.

---

## 🔧 Getting Started

### Prerequisites
- Docker and Docker Compose
- Rivus BI Discovery (`nifty_bell` container) running on the same host

### Running the Services
To launch the Data Lake and Sync Worker infrastructure in one command, simply run the provided startup script:

```bash
./start.sh
```

What `start.sh` does:
1. Boots up the MinIO Data Lake, Kafka, and the background worker via Docker Compose.
2. Automatically bridges the Rivus BI (`nifty_bell`) container network to the Data Lake so they can communicate.

### Accessing the Data Lake
You can manually inspect the Data Lake to see all synced CSV files:
- **MinIO Console**: `http://localhost:9001`
- **Username**: `admin`
- **Password**: `password123`

### Checking Logs
To ensure the worker is successfully looping and syncing:
```bash
docker-compose logs -f sync-worker
```
