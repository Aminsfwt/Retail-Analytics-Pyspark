from pyspark.sql.functions import col, sum, count, avg, round
from pyspark.sql import SparkSession
import argparse

def gold_layer(gold_input: str, daily_sales_output: str, product_category_performance_output: str, city_revenue_metrics_output: str):
    # Gold produces reporting-friendly aggregates from the cleaned silver dataset.
    with SparkSession.builder.master("spark://spark-master:7077").appName("Retail_Analytics_DockerClusterApp_GoldLayer").getOrCreate() as spark:
        spark.sparkContext.setLogLevel("ERROR")

        # Read the curated transaction-level dataset from the silver layer.
        silver_df = spark.read.parquet(gold_input)

        # Calculate net sales value after discount so all marts use the same revenue logic.
        silver_df = silver_df.withColumn(
            "total_amount",
            round(col("quantity") * col("unit_price") * (1 - col("discount_pct") / 100), 2)
        )

        # Build a daily summary for trend analysis and KPI dashboards.
        daily_sales_df = (
            silver_df
            .groupBy("order_date")
            .agg(
                round(sum("total_amount"), 2).alias("daily_total_revenue"),
                count("transaction_id").alias("daily_total_orders"),
                round(avg("total_amount"), 2).alias("daily_avg_order_value")
            )
        )

        # Persist the daily sales mart as parquet for BI and downstream consumption.
        (
            daily_sales_df
            .write
            .mode("overwrite")
            .parquet(daily_sales_output)
        )

        # Summarize category-level revenue and sales volume.
        product_perf_df = (
            silver_df
            .groupBy("product_category")
            .agg(
                round(sum("total_amount"), 2).alias("category_revenue"),
                sum("quantity").alias("total_units_sold"),
                count("transaction_id").alias("order_count")
            )
        )

        # Persist the category performance mart.
        (
            product_perf_df
            .write
            .mode("overwrite")
            .parquet(product_category_performance_output)
        )

        # Aggregate sales at city/state level to support regional analysis.
        city_revenue_df = (
            silver_df
            .groupBy("city", "state")
            .agg(
                round(sum("total_amount"), 2).alias("city_revenue"),
                count("transaction_id").alias("order_count"),
                round(avg("total_amount"), 2).alias("avg_order_value")
            )
        )

        # Persist the regional revenue mart.
        (
            city_revenue_df
            .write
            .mode("overwrite")
            .parquet(city_revenue_metrics_output)
        )

        print("Gold layer created successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create the gold layer of the data with Spark")
    parser.add_argument(
        "--gold_input",
        type=str,
        default="/opt/spark-data/silver/",
        help="Path to the silver parquet dataset",
    )
    parser.add_argument(
        "--daily_sales_output",
        type=str,
        default="/opt/spark-data/gold/daily_sales_metrics",
        help="Destination directory for the daily sales mart",
    )
    parser.add_argument(
        "--product_category_performance_output",
        type=str,
        default="/opt/spark-data/gold/product_category_performance",
        help="Destination directory for the product category performance mart",
    )
    parser.add_argument(
        "--city_revenue_metrics_output",
        type=str,
        default="/opt/spark-data/gold/city_revenue_metrics",
        help="Destination directory for the city revenue metrics mart",
    )
    
    args = parser.parse_args()

    gold_layer(
        args.gold_input,
        args.daily_sales_output,
        args.product_category_performance_output,
        args.city_revenue_metrics_output,
    )
