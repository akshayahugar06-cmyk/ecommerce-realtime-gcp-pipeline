from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "YOUR_PROJECT_ID"
REGION = "asia-south1"
PYSPARK_SCRIPT_URI = "gs://intricate-tempo-cleaned-ecom/scripts/clean_ecom.py"
BQ_CONNECTION_ID = (
    f"projects/{PROJECT_ID}/locations/{REGION}/connections/"
    "spanner-ecom-conn-v2"
)
BQ_TARGET_TABLE = (
    f"{PROJECT_ID}.ecom_analytics.daily_product_summary"
)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecom_lakehouse_orchestration_dag",
    default_args=default_args,
    description="Orchestrates Dataproc PySpark batch to BigQuery federated aggregation",
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "dataproc-serverless", "spanner", "bigquery"],
) as dag:

    batch_config = {
        "pyspark_batch": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
        },
        "runtime_config": {
            "version": "2.3",
            "properties": {
                "spark.jars.packages":
                    "com.google.cloud.spark.spanner:"
                    "spark-3.5-spanner:1.4.0",
                "spark.spanner.projectId": PROJECT_ID,
            },
        },
    }

    task_dataproc_batch = DataprocCreateBatchOperator(
        task_id="task_dataproc_pyspark_cleansing",
        project_id=PROJECT_ID,
        region=REGION,
        batch=batch_config,
        batch_id="clean-ecom-run-{{ ds_nodash }}-{{ ts_nodash.lower() }}",
    )

    federated_sql = f"""
    INSERT INTO `{BQ_TARGET_TABLE}`
    (product_id, total_items_sold, total_orders, aggregated_at)
    SELECT
        product_id,
        SUM(quantity) AS total_items_sold,
        COUNT(DISTINCT order_id) AS total_orders,
        CURRENT_TIMESTAMP() AS aggregated_at
    FROM EXTERNAL_QUERY(
        "{BQ_CONNECTION_ID}",
        "SELECT order_id, product_id, quantity FROM OrdersLineItems"
    )
    GROUP BY product_id;
    """

    task_bigquery_aggregation = BigQueryInsertJobOperator(
        task_id="task_bigquery_federated_analytics",
        configuration={
            "query": {
                "query": federated_sql,
                "useLegacySql": False,
            }
        },
        location=REGION,
        project_id=PROJECT_ID,
    )

    task_dataproc_batch >> task_bigquery_aggregation
