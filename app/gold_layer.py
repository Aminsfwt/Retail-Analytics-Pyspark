from pyspark.sql.functions import col, sum, count, avg, round
from pyspark.sql import SparkSession


# Set up Spark session
spark = SparkSession.builder \
    .appName("GoldLayer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read Silver Layer
silver_df = spark.read.parquet("/opt/spark-data/silver/retail_sales_clean.parquet")

# Derived Column: total_amount
silver_df = silver_df.withColumn(
    "total_amount",
    round(col("quantity") * col("unit_price") * (1 - col("discount_pct") / 100), 2)
)

# Daily Sales Metrics
daily_sales_df = (
    silver_df
    .groupBy("order_date")
    .agg(
        round(sum("total_amount"), 2).alias("daily_total_revenue"),
        count("transaction_id").alias("daily_total_orders"),
        round(avg("total_amount"), 2).alias("daily_avg_order_value")
    )
)

# Write Daily sales metrics to data mart
(
    daily_sales_df
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/gold/daily_sales_metrics.parquet")
)

# Product Category Performance
product_perf_df = (
    silver_df
    .groupBy("product_category")
    .agg(
        round(sum("total_amount"), 2).alias("category_revenue"),
        sum("quantity").alias("total_units_sold"),
        count("transaction_id").alias("order_count")
    )
)

# Write Product Category Performance to data mart
(
    product_perf_df
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/gold/product_category_performance.parquet")
)

# City-Level Revenue Metrics
city_revenue_df = (
    silver_df
    .groupBy("city", "state")
    .agg(
        round(sum("total_amount"), 2).alias("city_revenue"),
        count("transaction_id").alias("order_count"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    )
)

# Write City-Level Revenue Metrics to data mart
(
    city_revenue_df
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/gold/city_revenue_metrics.parquet")
)

print("Gold layer created successfully")