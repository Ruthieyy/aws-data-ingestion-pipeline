# AWS Data Ingestion Pipeline

Event-driven data ingestion pipeline using AWS S3 and Lambda.

## Architecture
## Tech Stack
- AWS S3 (data lake storage)
- AWS Lambda (serverless processing)
- AWS IAM (permissions)
- Python 3.14, boto3

## How it works
1. Upload any CSV file to S3 bucket
2. S3 automatically triggers Lambda function
3. Lambda reads, processes the file
4. Processed file saved to `processed/` folder in S3
