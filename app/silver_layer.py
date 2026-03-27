from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim
import argparse

def silver_layer(silver_input: str, silver_output: str):
    # Silver applies quality rules and standardization on top of the bronze dataset.
    with SparkSession.builder.master("spark://spark-master:7077").appName("SilverLayer").getOrCreate() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        
        # Read the parquet dataset produced by the bronze layer.
        bronze_df = spark.read.parquet(silver_input)

        # Basic logging helps confirm input size and schema before cleaning starts.
        print("Bronze count:", bronze_df.count())
        bronze_df.printSchema()

        # Inspect duplicate business keys before removing them from the curated layer.
        bronze_df.groupBy("transaction_id").count().filter(col("count") > 1)

        # Keep only one record per transaction_id to avoid double-counting in analytics.
        silver_df = bronze_df.dropDuplicates(["transaction_id"])

        # Log the row count after duplicate removal.
        print("Silver count after deduplication:", silver_df.count())

        # Shipping cannot happen before ordering, so invalid ship dates are nulled out.
        silver_df = silver_df.withColumn(
            "ship_date",
            when(col("ship_date") < col("order_date"), None)
            .otherwise(col("ship_date"))
        )

        # Rows with non-positive quantities are not valid sales transactions.
        silver_df = silver_df.filter(col("quantity") > 0)

        # Preserve the row but null out impossible pricing values for downstream handling.
        silver_df = silver_df.withColumn(
            "unit_price",
            when(col("unit_price") <= 0, None)
            .otherwise(col("unit_price"))
        )

        # Discounts must be between 0 and 100 percent.
        silver_df = silver_df.withColumn(
            "discount_pct",
            when((col("discount_pct") < 0) | (col("discount_pct") > 100), None)
            .otherwise(col("discount_pct"))
        )

        # Null out unrealistic ages while keeping the transaction itself.
        silver_df = silver_df.withColumn(
            "customer_age",
            when((col("customer_age") < 15) | (col("customer_age") > 100), None)
            .otherwise(col("customer_age"))
        )

        # Normalize gender labels into a compact, consistent set for analysis.
        silver_df = silver_df.withColumn(
            "gender",
            when(upper(trim(col("gender"))) == "MALE", "M")
            .when(upper(trim(col("gender"))) == "FEMALE", "F")
            .when(col("gender").isin("M", "F"), col("gender"))
            .otherwise(None)
        )

        # Keep only supported payment methods used by the reporting layer.
        silver_df = silver_df.withColumn(
            "payment_type",
            when(col("payment_type").isin("Card", "UPI", "COD"), col("payment_type"))
            .otherwise(None)
        )

        # Final row count after all silver quality rules are applied.
        print("Silver count:", silver_df.count())

        # Write the cleaned dataset for reuse by downstream marts.
        (
            silver_df
            .repartition(8)
            .write
            .mode("overwrite")
            .parquet(silver_output)
        )

        print("Silver layer created successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create the silver layer of the data with Spark")
    parser.add_argument(
        "--silver_input",
        type=str,
        default="/opt/spark-data/bronze/",
        help="Path to the bronze parquet dataset",
    )
    parser.add_argument(
        "--silver_output",
        type=str,
        default="/opt/spark-data/silver/",
        help="Destination directory for the cleaned silver parquet dataset",
    )
    args = parser.parse_args()

    silver_layer(args.silver_input, args.silver_output)
