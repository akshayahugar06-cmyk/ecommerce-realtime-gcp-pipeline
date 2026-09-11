# E-Commerce Real-Time GCP Data Pipeline

End-to-end e-commerce data pipeline POC using Google Cloud services. This project does not use Google Cloud Dataflow; Dataproc Serverless is used for PySpark transformation.

## GCP Services Used

- Google Cloud Pub/Sub — order-event ingestion
- Google Cloud Storage (GCS) — Bronze and Silver data layers
- Dataproc Serverless — PySpark transformation
- Cloud Spanner — operational data layer
- BigQuery — Gold analytics layer and federated querying
- BigQuery Spanner External Connection — Spanner-to-BigQuery federation
- Cloud Composer / Apache Airflow — workflow orchestration

## Pipeline Flow

Publisher → Pub/Sub → Bronze GCS → Cloud Composer/Airflow → Dataproc Serverless + PySpark
→ Silver GCS (Parquet) → Cloud Spanner → BigQuery EXTERNAL_QUERY → Gold table

> The source documents note that the Pub/Sub-to-Bronze-GCS delivery/subscription mechanism must be configured and confirmed before reproducing the pipeline end-to-end.

## Repository Structure

```text
ecommerce-realtime-gcp-pipeline/
├── README.md
├── docs/
│   ├── 01_GCP_REALTIME_PIPELINE_OVERVIEW.docx
│   ├── 02_GCP_PROJECT_SETUP_PUBSUB_GCS_DATAPROC_SPANNER_BIGQUERY_COMPOSER.docx
│   ├── 03_PUBSUB_GCS_DATAPROC_SPANNER_BIGQUERY_AIRFLOW_CODE.docx
│   └── 04_GCP_PIPELINE_SANITY_CHECKS.docx
├── scripts/
│   ├── publisher.py
│   ├── clean_ecom.py
│   └── ecom_lakehouse_orchestration_dag.py
└── .gitignore
```

## Document Execution Order

1. Read the pipeline overview.
2. Complete GCP project and infrastructure setup.
3. Configure and run the ingestion/transformation/orchestration code.
4. Run the sanity checks after the corresponding pipeline stages succeed.

## Project Values

Replace example values in the scripts with the values from your own GCP project before execution, including project ID, bucket names, Spanner resources, BigQuery dataset/table, region, Pub/Sub topic, external connection, and Composer environment.

## Notes

This repository is based on a proof-of-concept project. Review the setup documents before running the scripts in a production environment.
