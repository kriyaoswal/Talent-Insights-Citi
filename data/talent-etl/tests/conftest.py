"""Make ``job.py`` importable from the tests."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import socket  # noqa: E402

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def moto_endpoint():
    """A local S3 (moto server) for the whole session; no AWS account needed."""
    from moto.server import ThreadedMotoServer

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    yield f"http://127.0.0.1:{port}"
    server.stop()


@pytest.fixture
def s3(moto_endpoint, monkeypatch):
    """boto3 S3 client on moto; s3fs (used by job.py) picks the same endpoint from the environment."""
    import boto3
    import fsspec

    for key, value in {
        "AWS_ENDPOINT_URL_S3": moto_endpoint,
        "AWS_ENDPOINT_URL": moto_endpoint,
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    fsspec.filesystem("s3").clear_instance_cache()
    yield boto3.client("s3", endpoint_url=moto_endpoint, region_name="us-east-1")
    fsspec.filesystem("s3").clear_instance_cache()
