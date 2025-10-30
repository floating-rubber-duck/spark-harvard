#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/raw

echo "Downloading raw data..."

curl -L -o data/raw/yellow_tripdata_2025-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

curl -L -o data/raw/taxi_zone.csv \
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

echo "✅ Done. Raw data stored under data/raw/"