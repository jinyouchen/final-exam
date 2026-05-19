# Setup Guide
This document describes how to set up the development environment for the sensor data pipeline.

## Prerequisites
- Windows 10/11 with Git Bash/PowerShell
- Docker Desktop (WSL2 backend recommended)
- Python 3.10+ (with `venv`)
- Apache Spark 3.x (with winutils.exe for Windows)

## Step1: Deploy Kafka Cluster
1.  Navigate to the project root and run:
    ```bash
    docker compose up -d
Verify cluster health via Kafka UI at http://localhost:8080
Step2: Python Environment Setup
Create and activate a virtual environment:
powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
Install dependencies:
bash
pip install flask pyspark kafka-python
Step3: Spark Configuration
Set SPARK_HOME and HADOOP_HOME environment variables
Verify Spark installation:
powershell
spark-submit --version
Step4: Start Flask API
powershell
flask --app src/api/app.py run --port 5000
plaintext

### `docs/api.md`（Flask API 接口文档）
# API Documentation
All endpoints are prefixed with `http://localhost:5000`.

## 1. GET /health
Check API and backend service health.
- Response:
  ```json
  {
    "status": "healthy",
    "timestamp": "2026-05-19T10:00:00",
    "spark_connected": true,
    "kafka_connected": true
  }
2. GET /api/sensors
Get all available sensor types.
Response:
json
{
  "count": 3,
  "sensor_types": ["temperature", "pressure", "humidity"]
}
3. GET /api/latest/<sensor_type>
Get the latest reading for a specific sensor.
Example: /api/latest/temperature
Response includes value, timestamp, and anomaly status.
4. GET /api/stats/<sensor_type>
Get aggregated statistics (avg/max/min/anomaly rate) for a sensor.
5. GET /api/anomalies
Get anomalous sensor readings.
Query Params:
sensor_type (optional): Filter by sensor type
limit (optional, default=10): Number of records to return
6. POST /api/readings
Ingest a new sensor reading into Kafka.
Request Body (JSON):
json
{
  "sensor_type": "temperature",
  "value": 26.8,
  "unit": "C",
  "timestamp": 1747647360000,
  "source": "site-A-rack-12"
}
Response: 201 Created on success
plaintext

### `docs/pipeline.md`
# Pipeline Overview
This document describes the end-to-end data flow from Kafka to the data lake.

## 1. Kafka Ingestion
- Topic: `sensor-readings`
- Message Format: JSON with sensor metadata and value
- Retention Policy: 7 days (default)

## 2. Spark ETL Process
1.  Read stream data from Kafka
2.  Validate schema and clean malformed records
3.  Detect anomalies (values outside predefined ranges)
4.  Partition data by `sensor_type` and `date`
5.  Write to Parquet in the curated data lake

## 3. Data Lake Structure
Curated data is stored at `file:///tmp/datalake/curated` with the following partition structure:
curated/
├── sensor_type=temperature/
│ ├── date=2026-05-19/
│ └── date=2026-05-20/
└── sensor_type=pressure/
└── date=2026-05-19/