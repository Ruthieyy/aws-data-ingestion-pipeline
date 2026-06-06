# AWS Event-Driven Data Ingestion Pipeline

Serverless data ingestion pipeline built on AWS — automatically processes and stores data when files land in S3.

## Architecture

Raw CSV → S3 Upload → S3 Event Trigger → Lambda Processing → Processed Output (S3)

## Why I Built This

During my ML internship processing atmospheric sensor data, raw files were manually moved between systems. This project automates that ingestion layer using AWS-native serverless tools.

## Tech Stack

- **AWS S3** — data lake with date-based partitioning
- **AWS Lambda** — serverless processing, auto-triggered on upload
- **AWS IAM** — cross-service access control
- **Python / boto3** — data processing and AWS SDK

## How It Works

1. Drop any CSV into the S3 input bucket
2. S3 event automatically triggers Lambda
3. Lambda reads, validates, and transforms the file
4. Processed output saved to `processed/` folder

## Getting Started

```bash
git clone https://github.com/Ruthieyy/aws-data-ingestion-pipeline
```

Configure your AWS credentials, deploy the Lambda function, then upload a test CSV to trigger the pipeline.

## Related Projects

- [Data Quality Framework](https://github.com/Ruthieyy/data-quality-framework)
- [PySpark ETL Pipeline](https://github.com/Ruthieyy/pyspark-etl)
- [Airflow DAG Pipeline](https://github.com/Ruthieyy/airflow-demo)
