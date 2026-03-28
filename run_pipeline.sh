#!/bin/bash
set -e

echo "Starting Spark Cluster..."
docker-compose up -d spark-master spark-worker-1 spark-worker-2
sleep 10

echo "Preparing input data inside shared Spark volume..."
docker exec -u 0 spark-worker-1 sh -lc 'mkdir -p /opt/spark-data/raw && chmod -R a+rwx /opt/spark-data && cp /opt/spark-host-data/raw/retail_sales_raw.csv /opt/spark-data/raw/retail_sales_raw.csv && chmod -R a+rwx /opt/spark-data'
echo "Preparing input data inside shared Spark volume DONE..."

submit_job() {
  local script_path="$1"
  docker exec spark-worker-1 /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --conf spark.driver.host=spark-worker-1 \
    --conf spark.driver.bindAddress=0.0.0.0 \
    "$script_path"
}

echo "Running Bronze Layer..."
submit_job /opt/spark-app/bronze_layer.py
echo "Bronze Done"

echo "Running Silver Layer..."
submit_job /opt/spark-app/silver_layer.py
echo "Silver Done"

echo "Running Gold Layer..."
submit_job /opt/spark-app/gold_layer.py
echo "Gold Done"

echo "Exporting Bronze/Silver/Gold outputs to host ./data and cleaning container volume..."


for layer in bronze silver gold; do
  rm -rf "./data/$layer"
  docker cp "spark-worker-1:/opt/spark-data/$layer" "./data/"
  docker exec -u 0 spark-worker-1 sh -lc "rm -rf /opt/spark-data/$layer"
done

echo "Export complete at ./data"
echo "Pipeline Complete!"
