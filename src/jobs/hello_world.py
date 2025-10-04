"""
Simple hello world job to test deployment
"""
import argparse
from pyspark.sql import SparkSession
from datetime import datetime


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    # Create a simple test table
    data = [
        ("test", datetime.now(), "Hello from NASA Turbofan deployment!"),
    ]

    df = spark.createDataFrame(data, ["source", "timestamp", "message"])

    table_name = f"{args.catalog}.{args.schema}.deployment_test"

    print(f"Writing test data to {table_name}")
    df.write.mode("append").saveAsTable(table_name)

    print("Deployment test successful!")


if __name__ == "__main__":
    main()