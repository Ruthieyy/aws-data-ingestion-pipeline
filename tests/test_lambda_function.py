import json
from pathlib import Path
from urllib.parse import quote_plus

import boto3
import pytest
from moto import mock_aws

from lambda_function import CsvValidationError, lambda_handler, validate_csv

POLICY_PATH = Path(__file__).resolve().parents[1] / "iam_policy.json"
INPUT_BUCKET = "input-bucket"
OUTPUT_BUCKET = "output-bucket"


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.delenv("OUTPUT_BUCKET", raising=False)
    monkeypatch.setenv("INPUT_PREFIX", "raw/")
    monkeypatch.setenv("OUTPUT_PREFIX", "processed/")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=INPUT_BUCKET)
        yield client


def _event_key(key):
    """Encode a key the way S3 event notifications do (spaces as '+')."""
    return "/".join(quote_plus(part) for part in key.split("/"))


def _event(*keys, bucket=INPUT_BUCKET):
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": _event_key(key)},
                }
            }
            for key in keys
        ]
    }


def test_validate_csv_accepts_a_header_and_consistent_rows():
    assert validate_csv('city,note\n"Seattle","a,b"\n\n') == 1
    assert validate_csv("city,note\n") == 0


def test_validate_csv_rejects_empty_ragged_and_duplicate_headers():
    with pytest.raises(CsvValidationError, match="empty"):
        validate_csv(" \n")
    with pytest.raises(CsvValidationError, match="Row 2 has 1 column; header has 2"):
        validate_csv("city,note\nSeattle\n")
    with pytest.raises(CsvValidationError, match="Duplicate column"):
        validate_csv("city,city\nSeattle,Portland\n")


def test_valid_csv_is_copied_without_changing_bytes(s3):
    original = b'city,note\n"Seattle","a,b"\n'
    s3.put_object(Bucket=INPUT_BUCKET, Key="raw/sensors.csv", Body=original)

    result = lambda_handler(_event("raw/sensors.csv"), None)

    stored = s3.get_object(Bucket=INPUT_BUCKET, Key="processed/sensors.csv")
    body = stored["Body"].read()
    assert body == original
    assert body.splitlines()[0] == b"city,note"
    assert not body.startswith(b"processed=true")
    assert stored["ContentType"] == "text/csv"
    assert result["processed"][0]["data_rows"] == 1
    assert result["processed"][0]["output_key"] == "processed/sensors.csv"
    assert result["rejected"] == []


def test_every_record_is_handled_and_object_keys_are_url_decoded(s3):
    plain = b"id,value\n1,2\n"
    spaced = b"id,value\n3,4\n"
    s3.put_object(Bucket=INPUT_BUCKET, Key="raw/a.csv", Body=plain)
    s3.put_object(Bucket=INPUT_BUCKET, Key="raw/sensor readings.csv", Body=spaced)

    result = lambda_handler(
        _event("raw/a.csv", "raw/sensor readings.csv"),
        None,
    )

    assert s3.get_object(Bucket=INPUT_BUCKET, Key="processed/a.csv")["Body"].read() == plain
    assert (
        s3.get_object(Bucket=INPUT_BUCKET, Key="processed/sensor readings.csv")["Body"].read()
        == spaced
    )
    assert [item["source_key"] for item in result["processed"]] == [
        "raw/a.csv",
        "raw/sensor readings.csv",
    ]


def test_output_prefix_is_not_reprocessed(s3):
    original = b"id,value\n1,2\n"
    s3.put_object(Bucket=INPUT_BUCKET, Key="raw/sensors.csv", Body=original)

    first = lambda_handler(_event("raw/sensors.csv"), None)
    second = lambda_handler(_event("processed/sensors.csv"), None)

    keys = sorted(obj["Key"] for obj in s3.list_objects_v2(Bucket=INPUT_BUCKET)["Contents"])
    assert keys == ["processed/sensors.csv", "raw/sensors.csv"]
    assert first["processed"][0]["output_key"] == "processed/sensors.csv"
    assert second["skipped"] == [
        {
            "action": "skipped",
            "bucket": INPUT_BUCKET,
            "key": "processed/sensors.csv",
            "reason": "output-prefix",
        }
    ]
    assert second["processed"] == []


def test_invalid_csv_is_rejected_and_a_valid_sibling_is_still_copied(s3, caplog):
    caplog.set_level("ERROR")
    s3.put_object(Bucket=INPUT_BUCKET, Key="raw/broken.csv", Body=b"city,note\nSeattle\n")
    s3.put_object(Bucket=INPUT_BUCKET, Key="raw/ok.csv", Body=b"id\n1\n")

    result = lambda_handler(_event("raw/broken.csv", "raw/ok.csv"), None)

    keys = [
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix="processed/").get("Contents", [])
    ]
    assert keys == ["processed/ok.csv"]
    assert result["processed"][0]["source_key"] == "raw/ok.csv"
    assert result["rejected"][0]["reason"] == "Row 2 has 1 column; header has 2 columns"
    assert "Rejected" in caplog.text


def test_keys_outside_the_input_prefix_are_skipped(s3):
    s3.put_object(Bucket=INPUT_BUCKET, Key="incoming/sensors.csv", Body=b"id\n1\n")

    result = lambda_handler(_event("incoming/sensors.csv"), None)

    assert result["processed"] == []
    assert result["skipped"][0]["reason"] == "input-prefix"
    assert "Contents" not in s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix="processed/")


def test_refuses_to_write_when_output_prefix_is_under_the_input_prefix(s3, monkeypatch):
    monkeypatch.setenv("INPUT_PREFIX", "data")
    monkeypatch.setenv("OUTPUT_PREFIX", "data/out")
    s3.put_object(Bucket=INPUT_BUCKET, Key="data/file.csv", Body=b"id\n1\n")

    with pytest.raises(RuntimeError, match="Refusing to write"):
        lambda_handler(_event("data/file.csv"), None)

    assert "Contents" not in s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix="data/out/")


def test_missing_object_fails_the_invocation_after_other_records(s3):
    s3.put_object(Bucket=INPUT_BUCKET, Key="raw/ok.csv", Body=b"id\n1\n")
    event = _event("raw/missing.csv", "raw/ok.csv")

    with pytest.raises(RuntimeError, match="missing.csv"):
        lambda_handler(event, None)

    stored = s3.get_object(Bucket=INPUT_BUCKET, Key="processed/ok.csv")
    assert stored["Body"].read() == b"id\n1\n"


def test_output_bucket_env_writes_outside_the_source_bucket(s3, monkeypatch):
    monkeypatch.setenv("OUTPUT_BUCKET", OUTPUT_BUCKET)
    s3.create_bucket(Bucket=OUTPUT_BUCKET)
    original = b"id,value\n1,2\n"
    s3.put_object(Bucket=INPUT_BUCKET, Key="raw/sensors.csv", Body=original)

    result = lambda_handler(_event("raw/sensors.csv"), None)

    assert s3.get_object(Bucket=OUTPUT_BUCKET, Key="processed/sensors.csv")["Body"].read() == original
    assert "Contents" not in s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix="processed/")
    assert result["processed"][0]["output_bucket"] == OUTPUT_BUCKET


def test_iam_policy_scopes_s3_and_allows_function_logs():
    policy = json.loads(POLICY_PATH.read_text())
    actions = []
    resources = []
    for statement in policy["Statement"]:
        action = statement["Action"]
        resource = statement["Resource"]
        actions.extend(action if isinstance(action, list) else [action])
        resources.extend(resource if isinstance(resource, list) else [resource])

    assert actions.count("s3:GetObject") == 1
    assert actions.count("s3:PutObject") == 1
    assert "s3:ListBucket" not in actions
    assert "s3:DeleteObject" not in actions
    assert "logs:CreateLogGroup" in actions
    assert "logs:CreateLogStream" in actions
    assert "logs:PutLogEvents" in actions
    assert "arn:aws:s3:::YOUR_INPUT_BUCKET/raw/*" in resources
    assert "arn:aws:s3:::YOUR_OUTPUT_BUCKET/processed/*" in resources
    assert any(resource.endswith(":log-group:/aws/lambda/YOUR_FUNCTION_NAME:*") for resource in resources)
    assert "*" not in resources
