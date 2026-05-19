#!/bin/bash
API_BASE="http://localhost:5000"

echo "===== 1. Health Check ====="
curl -X GET $API_BASE/health
echo -e "\n"

echo "===== 2. Get All Sensor Types ====="
curl -X GET $API_BASE/api/sensors
echo -e "\n"

echo "===== 3. Get Latest Reading for pressure Sensor ====="
curl -X GET $API_BASE/api/latest/pressure
echo -e "\n"

echo "===== 4. Get Statistics for temperature Sensor ====="
curl -X GET $API_BASE/api/stats/temperature
echo -e "\n"

echo "===== 5. Get All Anomaly Data (Default 10 records) ====="
curl -X GET $API_BASE/api/anomalies
echo -e "\n"

echo "===== 6. Get Anomaly Data for humidity Sensor (Limit 5 records) ====="
curl -X GET "$API_BASE/api/anomalies?sensor_type=humidity&limit=5"
echo -e "\n"

echo "===== 7. POST New Sensor Reading ====="
curl -X POST $API_BASE/api/readings \
  -H "Content-Type: application/json" \
  -d '{
    "sensor_type": "temperature",
    "value": 26.8,
    "unit": "C",
    "timestamp": 1747647360000,
    "source": "site-A-rack-12"
  }'
echo -e "\n"