# Spark-Harvard: Data Engineering with Scala + Spark

Welcome!  
This repository contains a **complete end-to-end data pipeline** built in Scala using Spark, following the **medallion architecture** (Bronze → Silver → Gold).  

---

## What This Project Does

This project demonstrates how to build **clean, modular, and production-style data pipelines** using Spark.  
It includes:
- Automated data ingestion
- Data quality (DQ) validation
- Schema enforcement
- Incremental layering (Bronze → Silver → Gold)

We use **public NYC Taxi data** as the example dataset.

---

## Why You Need to Load the Data

The data files (Parquet + CSV) are **too large to store on GitHub.** GitHub limits file size to 50 MB.  
That’s why each collaborator must download them **locally** before running the code.

The script `scripts/fetch_data.sh` automates this for you. it:
1. Creates a `data/raw/` folder if it doesn’t exist.  
2. Downloads:
   - `yellow_tripdata_2025-01.parquet` (NYC trip records)
   - `taxi_zone_lookup.csv` (geographic zone metadata)
3. Saves them under `data/raw/`.

Once you have these files, all the Spark jobs will work immediately.

---

## Quick Start (New Collaborators)

### 1. Clone the Repository

git clone https://github.com/floating-rubber-duck/spark-harvard.git
cd spark-harvard

### 2. Download the Raw Data

Before running any jobs, download the sample NYC taxi data:

bash scripts/fetch_data.sh

### Your folder should look like this

data/
├── raw/
│   ├── yellow_tripdata_2025-01.parquet
│   └── taxi_zone.csv
├── bronze/
├── silver/
└── gold/

### 3. Then run the code

sbt run

This will execute all Bronze-layer jobs automatically.

To run a single job:

sbt "run yellow"    # Yellow tripdata pipeline
sbt "run taxi"      # Taxi zone pipeline

---

## Running with Docker

If you prefer to avoid installing sbt locally, you can use Docker.

Build and run:

docker build -t spark-harvard .
docker run --rm -v "$(pwd)/data:/app/data" spark-harvard

This will run the precompiled Spark job and write outputs to your local `data/` directory.

---

## Bronze Layer Overview

### BronzeTaxiZoneApp
- Reads `taxi_zone.csv`
- Cleans text, removes bad rows, enforces schema
- Outputs to `data/bronze/bronze_taxi_zone/`

### YellowTripdataBronzeApp
- Reads `yellow_tripdata_2025-01.parquet`
- Checks for nulls, invalid ranges, and missing zones
- Joins with the Taxi Zone dataset
- Splits results into:
  - `pass/` (valid)
  - `_rejects/` (invalid)
  - `_run_summary_*` (summary CSVs)

Example structure:

bronze_yellow_tripdata/
├── pass/
├── _rejects/
├── _run_summary_all/
├── _run_summary_counts/
└── _run_summary_nulls/

### YellowTripdataSilverApp
- Reads Bronze outputs (`bronze_yellow_tripdata/pass` + taxi zone lookup)
- Conforms schema, enriches pickup/dropoff geography, and appends KPI-ready columns
- Writes clean facts to `data/silver/silver_yellow_tripdata/curated/`
- Emits weekly run summaries under `_run_summary_weekly/`

Run it with either system property or CLI flag:

```
sbt "-Djob=yellow_trips_silver" run
# or
sbt "run --job yellow_trips_silver"
```

### Preview Silver Outputs
After the Silver job finishes, inspect the results (no spark-shell needed):

```
sbt "run --job show_silver"
# optional overrides:
# sbt "-DsilverPath=data/silver/silver_yellow_tripdata -DsampleSize=10" "run --job show_silver"
```

---

## Data Quality (DQ) Checks

Check | Description
------|-------------
Null percentage | Calculates % of missing data for each column
Range checks | Ensures numeric or date values are realistic
Referential integrity | Verifies pickup/dropoff zones exist
Schema validation | Detects missing or mismatched fields
Row-level tagging | Marks rejected records with failure reason

All summary CSVs are stored under `_run_summary_*` for traceability.

---

## Project Structure

spark-harvard/
├── build.sbt
├── Dockerfile
├── .gitignore
│
├── scripts/
│   └── fetch_data.sh
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   └── main/
│       └── scala/
│           ├── bronze/
│           │   ├── bronze_taxi_zone.scala
│           │   └── yellow_trips_bronze.scala
│           └── runner/
│               └── PipelineRunner.scala
│
└── project/

---

## Requirements

Tool | Version | Purpose
------|----------|---------
Java | 17+ | Runtime
Scala | 2.13.x | Main language
Apache Spark | 4.0.0 | Processing engine
sbt | 1.10+ | Build and dependency management
Docker | optional | Containerized runs

---

## Git Hygiene

Large data and build files are ignored to keep the repository clean.  
Your `.gitignore` already includes:

data/
!data/.keep
target/
*.jar
*.class
*.log

Only `.keep` files remain to preserve folder structure.

---

## For New Collaborators

If you just joined this repo, here’s your complete setup checklist:

git clone https://github.com/floating-rubber-duck/spark-harvard.git
cd spark-harvard
bash scripts/fetch_data.sh
sbt run

That’s all — the Bronze outputs will appear in `data/bronze/`.

---

## Future Plans

- Add Silver transformations and Gold aggregates  
- Introduce unit tests for validation logic  
- Integrate AWS Glue / Databricks / Redshift for production-scale jobs  

---

## License

MIT License © 2025 — Zachary Gacer

You are free to use, modify, and share this project for educational or research purposes.
