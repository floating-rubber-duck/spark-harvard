# 🧱 Spark-Harvard: Data Engineering with Scala + Spark

Welcome!  
This repository contains a **complete end-to-end data pipeline** built in Scala using  Spark**, following the **medallion architecture** (Bronze → Silver → Gold).  

---

## 🎯 What This Project Does

This project demonstrates how to build **clean, modular, and production-style data pipelines** using Spark.  
It includes:
- Automated data ingestion
- Data quality (DQ) validation
- Schema enforcement
- Incremental layering (Bronze → Silver → Gold)
- Optional Docker support for reproducibility

We use **public NYC Taxi data** as the example dataset.

---

## 🤔 Why You Need to Load the Data

The data files (Parquet + CSV) are **too large to store on GitHub** — GitHub limits file size to 50 MB.  
That’s why each collaborator must download them **locally** before running the code.

The script `scripts/fetch_data.sh` automates this for you — it:
1. Creates a `data/raw/` folder if it doesn’t exist.  
2. Downloads:
   - `yellow_tripdata_2025-01.parquet` (NYC trip records)
   - `taxi_zone_lookup.csv` (geographic zone metadata)
3. Saves them under `data/raw/`.

Once you have these files, all the Spark jobs will work immediately.

---

## 🚀 Quick Start (New Collaborators)

### 1️ Clone the Repository

```bash
git clone https://github.com/floating-rubber-duck/spark-harvard.git
cd spark-harvard

## 2️ Download the Raw Data

Before running any jobs, download the sample NYC taxi data:

```bash
bash scripts/fetch_data.sh

## Your folder should look like this

data/
├── raw/
│   ├── yellow_tripdata_2025-01.parquet
│   └── taxi_zone.csv
├── bronze/
├── silver/
└── gold/

## 3. Then run the code

sbt run
