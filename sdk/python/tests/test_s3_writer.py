"""Integration test for S3Writer using the e2e Go binary.

Usage::

    # Build the e2e binary first:
    cd /path/to/bytestack
    go build -o /tmp/e2e-server ./sdk/golang/e2e/cmd/e2e/

    # Run the test:
    python3 -m pytest sdk/python/tests/test_s3_writer.py -v
"""

from __future__ import annotations

import json
import os
import struct
import subprocess
from typing import Iterator

import boto3
import pytest

from bytestack_sdk import S3Writer  # noqa: E402

E2E_BINARY = os.environ.get("E2E_BIN", "/tmp/e2e-server")
BUCKET = "bst-test-bucket"


@pytest.fixture(scope="module")
def e2e_server() -> Iterator[dict[str, str]]:
    """Start the e2e binary and yield its endpoints as a dict.

    The dict has keys ``s3_endpoint`` and ``controller_addr``.
    The binary is killed when the fixture cleans up.
    """
    proc = subprocess.Popen(
        [E2E_BINARY],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Read the first JSON line from stdout.
    stdout_line = proc.stdout.readline()
    try:
        info = json.loads(stdout_line.decode("utf-8").strip())
    except Exception as exc:
        proc.kill()
        proc.wait()
        pytest.fail(f"failed to parse e2e startup JSON: {exc}")

    yield info

    # Cleanup.
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture(scope="module")
def aws_test_env(e2e_server: dict[str, str]) -> Iterator[None]:
    """Set fake AWS credentials and endpoint for the whole test module."""
    saved_env = {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.environ.get("AWS_REGION"),
        "BYTESTACK_S3_ENDPOINT": os.environ.get("BYTESTACK_S3_ENDPOINT"),
    }
    os.environ["AWS_ACCESS_KEY_ID"] = "dummy-access-key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "dummy-secret-key"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["BYTESTACK_S3_ENDPOINT"] = e2e_server["s3_endpoint"]

    try:
        yield
    finally:
        for k, v in saved_env.items():
            if v is not None:
                os.environ[k] = v
            else:
                os.environ.pop(k, None)


@pytest.fixture(scope="module")
def s3_client(e2e_server: dict[str, str]) -> boto3.client:
    """Create a boto3 S3 client pointed at the fake S3 server."""
    from botocore.config import Config as BotoConfig

    client = boto3.client(
        "s3",
        endpoint_url=e2e_server["s3_endpoint"],
        config=BotoConfig(s3={"addressing_style": "path"}),
        aws_access_key_id="dummy-access-key",
        aws_secret_access_key="dummy-secret-key",
        region_name="us-east-1",
    )
    return client


@pytest.fixture(scope="module")
def s3_bucket(s3_client: boto3.client) -> str:
    """Create and yield the test bucket name."""
    s3_client.create_bucket(Bucket=BUCKET)
    return BUCKET


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_s3_writer_put_and_close(
    e2e_server: dict[str, str],
    aws_test_env: None,
    s3_client: boto3.client,
    s3_bucket: str,
):
    """Write records via S3Writer and verify objects on S3."""
    location = f"s3://{s3_bucket}/stacks"
    controller = e2e_server["controller_addr"]
    w = S3Writer.open(location, controller=controller)
    assert w.location == location

    sid = w.stack_id
    # Mock controller returns sequential IDs starting from 1.
    assert sid == 1, f"expected stack_id=1, got {sid}"

    # --- Write two records ----------------------------------------------
    id1 = w.put(b"hello s3", "greeting.txt")
    assert isinstance(id1, str)
    assert id1.startswith(f"{sid},")

    id2 = w.put(b"second record", "second.txt", extra_meta=b'{"k":"v"}')
    assert isinstance(id2, str)
    assert id2.startswith(f"{sid},")
    assert id1 != id2, "two puts must return different index_ids"

    w.close()

    # --- Verify objects exist ------------------------------------------
    for suffix in ("data", "idx", "meta"):
        key = f"stacks/0x{sid:04x}.{suffix}"
        obj = s3_client.head_object(Bucket=s3_bucket, Key=key)
        assert obj["ResponseMetadata"]["HTTPStatusCode"] == 200, (
            f"object s3://{s3_bucket}/{key} not found"
        )

    # --- Verify header contains real stack_id (not u64::MAX) -----------
    for suffix in ("data", "idx"):
        key = f"stacks/0x{sid:04x}.{suffix}"
        obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
        hdr = obj["Body"].read(16)
        obj["Body"].close()
        stored_id = struct.unpack("<QQ", hdr)[1]
        assert stored_id == sid, (
            f"{suffix} header stack_id = {stored_id:#x}, want {sid:#x}"
        )

    # --- Verify meta file header ---------------------------------------
    meta_key = f"stacks/0x{sid:04x}.meta"
    meta_obj = s3_client.get_object(Bucket=s3_bucket, Key=meta_key)
    meta_header = json.loads(meta_obj["Body"].readline())
    meta_obj["Body"].close()
    assert meta_header["stack_id"] == sid, (
        f"meta header stack_id = {meta_header['stack_id']:#x}, want {sid:#x}"
    )

    # --- Put after close must raise ------------------------------------
    with pytest.raises(Exception):
        w.put(b"late", "late.txt")


def test_s3_writer_without_controller_fails():
    """S3Writer requires a controller address."""
    with pytest.raises(TypeError):
        S3Writer.open("s3://my-bucket/my-prefix")


def test_s3_writer_non_s3_location_fails():
    """S3Writer requires an s3:// location."""
    with pytest.raises(ValueError, match="s3://"):
        S3Writer.open("/tmp/mystack", controller="http://localhost:8080")
