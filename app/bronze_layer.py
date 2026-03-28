from pyspark.sql import SparkSession, types
import argparse

def bronze_layer(input: str, output: str):
    # Bronze keeps the raw dataset structure intact while converting CSV to parquet.
    with SparkSession.builder.master("spark://spark-master:7077").appName("Retail_Analytics_DockerClusterApp_BronzeLayer").getOrCreate() as spark:
        spark.sparkContext.setLogLevel("ERROR")

        print("\n \n starting the job!")

        # Use an explicit schema so Spark does not infer inconsistent types from raw CSV values.
        right_schema = types.StructType([
            types.StructField('transaction_id', types.StringType(), True), 
            types.StructField('order_date', types.DateType(), True), 
            types.StructField('ship_date', types.DateType(), True), 
            types.StructField('customer_id', types.StringType(), True), 
            types.StructField('customer_age', types.IntegerType(), True), 
            types.StructField('gender', types.StringType(), True), 
            types.StructField('product_id', types.StringType(), True), 
            types.StructField('product_category', types.StringType(), True), 
            types.StructField('quantity', types.IntegerType(), True), 
            types.StructField('unit_price', types.FloatType(), True), 
            types.StructField('discount_pct', types.FloatType(), True), 
            types.StructField('city', types.StringType(), True), 
            types.StructField('state', types.StringType(), True), 
            types.StructField('payment_type', types.StringType(), True), 
            types.StructField('order_status', types.StringType(), True), 
            types.StructField('ingestion_date', types.DateType(), True)]
        )

        # Read the raw file exactly as delivered, preserving all rows for downstream cleaning.
        bronze_df = spark.read.schema(right_schema).csv(input, header=True)
        print(bronze_df.count())

        print("start writing")
        # Repartition before writing so the bronze output is distributed across the cluster.
        bronze_df.repartition(24).write.mode("overwrite").parquet(output)

    print("Bronze layer created successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create the bronze layer of the data with Spark")
    parser.add_argument(
        "--input",
        type=str,
        default="/opt/spark-data/raw/retail_sales_raw.csv",
        help="Path to the raw retail sales CSV file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="/opt/spark-data/bronze",
        help="Destination directory for the bronze parquet dataset",
    )
    args = parser.parse_args()

    bronze_layer(args.input, args.output)
