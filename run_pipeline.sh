#!/bin/bash
set -e

echo "Starting Spark Cluster..."
docker-compose up -d spark-master spark-worker-1 spark-worker-2
sleep 10

SUBMIT="docker exec spark-worker-1 /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --conf spark.driver.host=spark-worker-1 \
    --conf spark.driver.bindAddress=0.0.0.0"

echo "Running Bronze Layer..."
$SUBMIT /opt/spark-app/bronze_layer.py
echo "Bronze Done"

echo "Running Silver Layer..."
$SUBMIT /opt/spark-app/silver_layer.py
echo "Silver Done"

echo "Running Gold Layer..."
$SUBMIT /opt/spark-app/gold_layer.py
echo "Gold Done"

echo "Pipeline Complete!"
