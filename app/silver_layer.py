from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SilverLayer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read Bronze Layer
bronze_df = spark.read.parquet("/opt/spark-data/bronze/retail_sales_bronze.parquet")

# Initial Data Exploration
print("Bronze count:", bronze_df.count())
bronze_df.printSchema()

# Show Deduplicated records
bronze_df.groupBy("transaction_id").count().filter(col("count") > 1)

# Drop Duplicates
silver_df = bronze_df.dropDuplicates(["transaction_id"])

# Show count after deduplication
print("Bronze count:", silver_df.count())

# Date Corrections
# Set ship_date to null where it is before order_date
silver_df = silver_df.withColumn(
    "ship_date",
    when(col("ship_date") < col("order_date"), None)
    .otherwise(col("ship_date"))
)

# Quantity & Price Cleaning
silver_df = silver_df.filter(col("quantity") > 0)

# Set unit_price to null where it is <= 0
silver_df = silver_df.withColumn(
    "unit_price",
    when(col("unit_price") <= 0, None)
    .otherwise(col("unit_price"))
)

# Discount Cleaning
silver_df = silver_df.withColumn(
    "discount_pct",
    when((col("discount_pct") < 0) | (col("discount_pct") > 100), None)
    .otherwise(col("discount_pct"))
)

# Customer Age Cleaning
silver_df = silver_df.withColumn(
    "customer_age",
    when((col("customer_age") < 15) | (col("customer_age") > 100), None)
    .otherwise(col("customer_age"))
)

# Standardize Gender
silver_df = silver_df.withColumn(
    "gender",
    when(upper(trim(col("gender"))) == "MALE", "M")
    .when(upper(trim(col("gender"))) == "FEMALE", "F")
    .when(col("gender").isin("M", "F"), col("gender"))
    .otherwise(None)
)

# Standardize Payment Type
silver_df = silver_df.withColumn(
    "payment_type",
    when(col("payment_type").isin("Card", "UPI", "COD"), col("payment_type"))
    .otherwise(None)
)

# Final count after cleaning
print("Silver count:", silver_df.count())

# Write Silver Layer
(
    silver_df
    .repartition(8)
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/silver/retail_sales_clean.parquet")
)

print("Silver layer created successfully")