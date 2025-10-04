"""
Minimal DLT pipeline for testing
"""
import dlt
from pyspark.sql.functions import current_timestamp, lit

@dlt.table(
    name="test_bronze",
    comment="Test table for deployment verification"
)
def test_bronze():
    return (
        spark.range(10)
        .withColumn("timestamp", current_timestamp())
        .withColumn("message", lit("Test deployment"))
    )