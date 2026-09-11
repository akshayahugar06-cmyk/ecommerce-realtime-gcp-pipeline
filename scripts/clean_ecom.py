import sys
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp

def main():
    spark = (
        SparkSession.builder
        .appName("ECom-Airflow-Lakehouse-Clean")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    project_id = "YOUR_PROJECT_ID"
    raw_bucket_name = "intricate-tempo-raw-ecom"
    cleaned_parquet_dest = "gs://intricate-tempo-cleaned-ecom/parquet/"
    spanner_instance_id = "ecom-spanner-instance"
    spanner_database_id = "ecom-prod-db"
    spanner_table_name = "OrdersLineItems"

    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(raw_bucket_name)

        blobs = list(bucket.list_blobs(prefix="raw_order_"))
        if not blobs:
            print(f"[WARNING] No raw files found in gs://{raw_bucket_name}/")
            return

        json_payloads = [
            blob.download_as_text().strip()
            for blob in blobs
            if blob.download_as_text().strip()
        ]

        if not json_payloads:
            print("[WARNING] All downloaded raw files were empty.")
            return

        raw_rdd = spark.sparkContext.parallelize(json_payloads)
        raw_df = spark.read.json(raw_rdd)

        flattened_df = (
            raw_df
            .filter(col("id").isNotNull())
            .withColumn("product", explode("products"))
            .select(
                col("id").cast("string").alias("order_id"),
                col("userId").cast("string").alias("user_id"),
                col("product.productId").cast("string").alias("product_id"),
                col("product.quantity").cast("long").alias("quantity"),
                to_timestamp(col("date")).alias("order_date")
            )
            .dropDuplicates(["order_id", "product_id"])
        )

        flattened_df.write.mode("overwrite").parquet(cleaned_parquet_dest)

        (
            flattened_df.write
            .format("cloud-spanner")
            .option("projectId", project_id)
            .option("instanceId", spanner_instance_id)
            .option("databaseId", spanner_database_id)
            .option("table", spanner_table_name)
            .mode("append")
            .save()
        )

        print("[SUCCESS] Cleansed records written to Parquet and Spanner!")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
Code hint: The source code uses the Bronze prefix raw_order_. The Pub/Sub-to-GCS delivery mechanism must create object names matching that prefix, or clean_ecom.py must be adjusted consistently.
