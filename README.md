# Rivus BI to AI Link (bi-ai-link)

Welcome! This repository acts as the magical "Bridge" between your business intelligence data and artificial intelligence. 

Specifically, this repository links **[Rivus BI](https://github.com/mouuussab/rivus-bi)** (where you prepare and clean your data) to **[Rivus AI](https://github.com/mouuussab/rivus-ai)** (where you chat with your data and generate amazing charts).

If you are a complete beginner, this guide will explain everything you need to know in plain English!

## What does this actually do?

When you clean your data in Rivus BI and click "Run" to create a snapshot, that data gets locked inside a specialized database (Apache Druid). Rivus AI cannot read that database directly. 

This repository runs two things to fix that:
1. **MinIO Data Lake**: A localized file storage system (similar to Amazon S3 or Google Drive) that runs directly on your computer.
2. **The Sync Worker**: A smart Python robot that runs constantly in the background. Every 3 seconds, it checks Rivus BI to see if you have created any new data snapshots. If it finds new data, it extracts it, cleans up the filename, and saves it as a crisp `.csv` file into the MinIO Data Lake.

Once the data is in the Data Lake, **Rivus AI** can instantly fetch it and start chatting with it!

## Prerequisites

You only need **Docker** installed on your computer. Docker is a tool that allows these programs to run inside isolated "containers" so you don't have to install Python or databases on your personal machine.
* [Download Docker Desktop](https://www.docker.com/products/docker-desktop/)

## How to Start the Bridge

We have made this incredibly easy. You do not need to type any complex Docker commands.

1. **Open your terminal** and navigate to this folder:
   ```bash
   cd /path/to/bi-ai-link
   ```

2. **Start the bridge** by running the start script:
   ```bash
   ./start.sh
   ```
   *What this script does:* 
   - It turns on the MinIO Data Lake.
   - It creates a dedicated "bucket" (folder) called `rivus-data` for your files.
   - It starts the Python Sync Worker to begin watching Rivus BI.

3. **Verify it is working**
   You can view your Data Lake by opening your web browser and going to:
   ```
   http://localhost:9001
   ```
   *Login:* `admin` / `password123`
   
   If you look inside the `rivus-data` bucket, you will see your Rivus BI datasets sitting there as `.csv` files! You can now copy the download link for any of these files and paste it into **Rivus AI** to start chatting.

## How to Stop the Bridge

When you are completely finished working, you can safely turn the bridge off.

1. In your terminal, run:
   ```bash
   ./stop.sh
   ```
   This will cleanly shut down the Data Lake and the Sync Worker. Your data is perfectly safe and will still be there the next time you click start!

## Ecosystem Repositories

This project requires its two sibling projects to function fully. Make sure you check them out:
* **[rivus-bi](https://github.com/mouuussab/rivus-bi)**: Start here to upload and prepare your data.
* **[rivus-ai](https://github.com/mouuussab/rivus-ai)**: End here to chat with your prepared data and generate beautiful visualizations.
