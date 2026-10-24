# S3 CSV ingestion Lambda

A small AWS Lambda function that runs when a CSV lands in S3. It checks that the object is a usable CSV, then copies the original bytes to a processed prefix. It does not change the file contents.

Raw CSV uploaded to `raw/` → S3 `ObjectCreated` → Lambda → copy under `processed/`

## What it does

For each record in the event the function:

1. URL-decodes the object key (`+` and `%20` both become a space).
2. Skips the object unless the key starts with the input prefix (default `raw/`) and ends with `.csv`.
3. Skips keys that already start with the output prefix (default `processed/`), so an output write is not processed again.
4. Reads the object and checks that it is UTF-8 text (a leading BOM is accepted and kept) and a CSV with a header, unique non-blank column names, and the same number of columns on every data row. Blank lines are ignored. A header with no data rows is accepted.
5. Copies the original bytes to `processed/<path after raw/>` and sets `Content-Type` to `text/csv`.

Invalid CSVs are logged and left in `raw/`. The invocation still succeeds for those records, because retrying would not fix the file. If S3 returns an error, or a record is malformed, the invocation fails after the other records are attempted. Writing the same output key again overwrites it.

Environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `INPUT_PREFIX` | `raw/` | Only keys with this prefix are read. |
| `OUTPUT_PREFIX` | `processed/` | Destination prefix. Must not sit under `INPUT_PREFIX` when the output bucket is the source bucket. |
| `OUTPUT_BUCKET` | source bucket | Set this to write to a different bucket. |

A trailing slash is added when a prefix does not have one. The handler is `lambda_function.lambda_handler`. The Lambda runtime supplies `boto3`; this repo has no extra runtime dependencies.

## Recursion guard

Output is written to a different prefix, and the code never reads that prefix. Set the bucket notification as well, so output objects are not sent to the function in the first place:

- Events: `s3:ObjectCreated:*`
- Prefix: `raw/` (the same value as `INPUT_PREFIX`)
- Suffix: `.csv`
- Destination: this function

Do not add a notification for `processed/`. With the defaults, a same-bucket layout cannot loop: `processed/...` does not start with `raw/`, and the handler skips it without writing. Setting `OUTPUT_BUCKET` moves the copy to another bucket, which also avoids a notification on the source bucket.

## Deploy

1. Create the input bucket. Use a second bucket only if you set `OUTPUT_BUCKET`.
2. Create a Lambda function on Python 3.12 and upload `lambda_function.py`. Set the handler to `lambda_function.lambda_handler`.
3. Replace the placeholders in `iam_policy.json` and attach it to the function's execution role.
   - `YOUR_INPUT_BUCKET` / `YOUR_OUTPUT_BUCKET`: use the same name twice when output stays in the source bucket. `GetObject` is limited to `raw/*`. `PutObject` is limited to `processed/*`. The function does not list or delete objects.
   - `YOUR_REGION`, `YOUR_ACCOUNT_ID`, `YOUR_FUNCTION_NAME`: log-stream writes are limited to this function's log group. `logs:CreateLogGroup` is granted on the account and region because the group does not exist on the first invoke.
   - If you change `INPUT_PREFIX` or `OUTPUT_PREFIX`, change those resource ARNs to match.
4. Allow S3 to invoke the function, then add the notification (prefix `raw/`, suffix `.csv`):

```bash
aws lambda add-permission \
  --function-name YOUR_FUNCTION_NAME \
  --statement-id s3-invoke-raw-csv \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::YOUR_INPUT_BUCKET \
  --source-account YOUR_ACCOUNT_ID

# This replaces the bucket's whole notification configuration.
aws s3api put-bucket-notification-configuration \
  --bucket YOUR_INPUT_BUCKET \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [
      {
        "Id": "raw-csv",
        "LambdaFunctionArn": "arn:aws:lambda:YOUR_REGION:YOUR_ACCOUNT_ID:function:YOUR_FUNCTION_NAME",
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [
              {"Name": "prefix", "Value": "raw/"},
              {"Name": "suffix", "Value": ".csv"}
            ]
          }
        }
      }
    ]
  }'
```

5. Upload a file under the input prefix, for example `s3://YOUR_INPUT_BUCKET/raw/sensors.csv`:

```csv
city,reading
Seattle,1
```

The copy is `processed/sensors.csv` on the output bucket, with the same bytes. That bucket is the input bucket when `OUTPUT_BUCKET` is unset.

## Test

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

The tests use moto. They check byte-preserving copies, URL-decoded keys, multiple records, the prefix guards, rejected CSVs, an S3 error failing the invocation, a separate output bucket, and the IAM statement scope.

## Limitations / Next steps

- Structural checks only. There is no type parsing, schema, column transform, or date partitioning.
- Rejected files stay in `raw/` and are only written to CloudWatch Logs. A quarantine prefix or dead-letter queue is not set up.
- The whole object is loaded into memory. This fits small CSV drops, not multi-GB files.
- The bucket notification is not defined in this repo. It has to be configured as shown above (or with Terraform, SAM, or CDK).
- A bad file does not fail the invocation. An S3 or event error does, and Lambda will retry that invocation.

## Related projects

- [Data Quality Framework](https://github.com/Ruthieyy/data-quality-framework)
- [PySpark ETL Pipeline](https://github.com/Ruthieyy/pyspark-etl)
- [Airflow DAG Pipeline](https://github.com/Ruthieyy/airflow-demo)
