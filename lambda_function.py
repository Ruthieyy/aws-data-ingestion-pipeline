"""Validate a raw CSV dropped in S3 and copy the original bytes to a processed prefix.

The function is meant to be invoked by an S3 ObjectCreated notification filtered
to the input prefix and ``.csv`` suffix. It only reads keys under the input
prefix and writes under a different prefix (or a different bucket), so an
output write cannot trigger another copy.
"""

import csv
import io
import logging
import os
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_INPUT_PREFIX = "raw/"
DEFAULT_OUTPUT_PREFIX = "processed/"


class CsvValidationError(ValueError):
    """The object is not an acceptable CSV. Retrying will not help."""


def lambda_handler(event, context):
    """Process every S3 record in the event.

    Invalid CSVs are logged and reported as rejected. They do not fail the
    invocation, because a retry would keep failing the same object. Errors
    talking to S3, or a malformed record, fail the invocation after the other
    records have been attempted. Rewriting an output key is idempotent.
    """
    if not isinstance(event, dict):
        raise ValueError("Event must be a dict with a Records list")

    records = event.get("Records") or []
    request_id = getattr(context, "aws_request_id", None)
    logger.info("Start request_id=%s records=%d", request_id, len(records))

    if not records:
        logger.warning("Event contained no S3 records")
        return _result([], [], [])

    s3 = boto3.client("s3")
    processed = []
    skipped = []
    rejected = []
    operational_errors = []

    for record in records:
        hint = _record_hint(record)
        try:
            outcome = process_record(s3, record)
        except CsvValidationError as exc:
            logger.error("Rejected %s: %s", hint, exc)
            rejected.append({"key": hint, "reason": str(exc)})
            continue
        except (ClientError, BotoCoreError, KeyError, TypeError) as exc:
            logger.exception("Operational failure for %s", hint)
            operational_errors.append({"key": hint, "reason": str(exc)})
            continue

        if outcome["action"] == "processed":
            processed.append(outcome)
        else:
            skipped.append(outcome)

    if operational_errors:
        raise RuntimeError(
            f"Failed {len(operational_errors)} of {len(records)} record(s): "
            f"{operational_errors}"
        )

    logger.info(
        "Done processed=%d skipped=%d rejected=%d",
        len(processed),
        len(skipped),
        len(rejected),
    )
    return _result(processed, skipped, rejected)


def process_record(s3, record):
    """Validate one uploaded object and copy it, or skip it."""
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])
    input_prefix = _prefix_from_env("INPUT_PREFIX", DEFAULT_INPUT_PREFIX)
    output_prefix = _prefix_from_env("OUTPUT_PREFIX", DEFAULT_OUTPUT_PREFIX)
    output_bucket = os.environ.get("OUTPUT_BUCKET", "").strip() or bucket

    if key.startswith(output_prefix):
        logger.info(
            "Skipping s3://%s/%s; output prefix %s is not reprocessed",
            bucket,
            key,
            output_prefix,
        )
        return _skip(bucket, key, "output-prefix")

    if not key.startswith(input_prefix):
        logger.info(
            "Skipping s3://%s/%s; key is outside input prefix %s",
            bucket,
            key,
            input_prefix,
        )
        return _skip(bucket, key, "input-prefix")

    if not key.lower().endswith(".csv"):
        logger.info("Skipping non-CSV key s3://%s/%s", bucket, key)
        return _skip(bucket, key, "suffix")

    output_key = output_prefix + key[len(input_prefix):]
    if output_bucket == bucket and output_key.startswith(input_prefix):
        raise RuntimeError(
            "Refusing to write "
            f"s3://{output_bucket}/{output_key} under input prefix {input_prefix}"
        )

    logger.info("Reading s3://%s/%s", bucket, key)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError("Object is not valid UTF-8") from exc

    data_rows = validate_csv(text)
    s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=body,
        ContentType="text/csv",
    )
    logger.info(
        "Wrote s3://%s/%s data_rows=%d",
        output_bucket,
        output_key,
        data_rows,
    )
    return {
        "action": "processed",
        "source_bucket": bucket,
        "source_key": key,
        "output_bucket": output_bucket,
        "output_key": output_key,
        "data_rows": data_rows,
    }


def validate_csv(text):
    """Return the number of data rows.

    Accepts a non-empty CSV with a header, unique non-blank column names, and
    a consistent column count. Blank lines are ignored. The caller stores the
    original bytes; this function does not rewrite the file.
    """
    if not isinstance(text, str) or not text.strip():
        raise CsvValidationError("CSV is empty")

    try:
        rows = list(csv.reader(io.StringIO(text)))
    except csv.Error as exc:
        raise CsvValidationError(f"CSV could not be parsed: {exc}") from exc

    header = None
    data_rows = 0
    width = 0
    for line_no, row in enumerate(rows, start=1):
        if len(row) == 0 or (len(row) == 1 and row[0].strip() == ""):
            continue
        if header is None:
            header = row
            width = len(header)
            _validate_header(header)
            continue
        if len(row) != width:
            raise CsvValidationError(
                f"Row {line_no} has {_column_count(len(row))}; "
                f"header has {_column_count(width)}"
            )
        data_rows += 1

    if header is None:
        raise CsvValidationError("CSV header is missing")
    return data_rows


def _column_count(count):
    noun = "column" if count == 1 else "columns"
    return f"{count} {noun}"


def _validate_header(header):
    seen = set()
    for name in header:
        label = name.strip()
        if not label:
            raise CsvValidationError("CSV header contains a blank column name")
        if label in seen:
            raise CsvValidationError(f"Duplicate column name: {label}")
        seen.add(label)


def _prefix_from_env(name, default):
    value = os.environ.get(name, "").strip() or default
    if not value.endswith("/"):
        value += "/"
    return value


def _skip(bucket, key, reason):
    return {"action": "skipped", "bucket": bucket, "key": key, "reason": reason}


def _result(processed, skipped, rejected):
    return {
        "statusCode": 200,
        "processed": processed,
        "skipped": skipped,
        "rejected": rejected,
    }


def _record_hint(record):
    if not isinstance(record, dict):
        return "<unreadable record>"
    try:
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(str(record["s3"]["object"]["key"]))
    except (KeyError, TypeError, AttributeError):
        return "<unreadable record>"
    return f"s3://{bucket}/{key}"
