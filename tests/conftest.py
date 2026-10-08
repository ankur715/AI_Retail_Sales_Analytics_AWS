import os
import sys
from pathlib import Path

# Tests never touch real AWS: fake credentials + a fixed bucket name, set
# before etl.config is imported anywhere.
os.environ.update({
    "RETAIL_ENV": "dev",
    # LLM features off unless a test opts in; no real keys (a developer's .env may hold them)
    "LLM_PROVIDER": "none",
    "GOOGLE_API_KEY": "",
    "GEMINI_MODEL": "gemini-test",
    "BEDROCK_MODEL": "claude-opus-5-5",
    "LLM_BEDROCK_ENDPOINT": "mantle",
    # failure email off: no SMTP in tests (a developer's .env may configure it)
    "SMTP_USER": "",
    "SMTP_PASSWORD": "",
    "ALERT_EMAIL": "",
    "S3_BUCKET": "test-retail-lake",
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_REGION": "us-east-1",
})
os.environ.pop("AWS_PROFILE", None)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def no_redshift(monkeypatch):
    """Unit tests must never reach a real database. A developer's .env holds
    live Redshift credentials, so a test that forgets to fake a call would
    otherwise pass locally (by querying Redshift) and fail only in CI."""
    from etl import redshift

    def refuse(*args, **kwargs):
        raise RuntimeError("unit test tried to connect to Redshift -- fake the call instead")

    monkeypatch.setattr(redshift, "get_connection", refuse)
