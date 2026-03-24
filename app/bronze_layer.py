from pyspark.sql import SparkSession, types


# Start spark session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Retail_Analytics_DockerClusterApp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n \n \n \n starting the job! \n \n \n \n")

# Create the right schema for the raw CSV data
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

# read the raw CSV data with the right schema
bronze_df = spark.read.schema(right_schema).csv("/opt/spark-data/raw/retail_sales_raw.csv", header=True)

# write the data to bronze layer in parquet format
bronze_df.repartition(24).write.mode("overwrite").parquet("/opt/spark-data/bronze/retail_sales_bronze.parquet")

print("\n \n \n \n Bronze layer created successfully! \n \n \n \n")
