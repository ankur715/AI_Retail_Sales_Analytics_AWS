"""All runtime configuration comes from environment variables (.env locally,
Airflow Variables/Connections or Secrets Manager in a real deployment).
Nothing secret is ever hardcoded or committed.

RETAIL_ENV is the one switch between development and production. The same
code runs in both; the environment only changes WHERE data goes:

    env    Redshift database   S3 prefix            DAGs
    dev    retail_dev          s3://<bucket>/dev/   paused on creation, history loads allowed
    prod   retail_prod         s3://<bucket>/prod/  scheduled, rolling 5-week upsert
"""
import os

from dotenv import load_dotenv

# Load the project's .env (one folder up from etl/) no matter where the
# script is launched from. Real environment variables win over .env, so
# Airflow/CI can override anything.
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

ENVIRONMENTS = ("dev", "prod")

ENV = os.environ.get("RETAIL_ENV", "dev").lower()
if ENV not in ENVIRONMENTS:
    raise ValueError(f"RETAIL_ENV must be one of {ENVIRONMENTS}, got {ENV!r}")

# --- AWS ---
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "")          # terraform output: s3_bucket
S3_ENV_PREFIX = ENV                                   # every key in this env starts with dev/ or prod/

# --- Redshift Serverless (Postgres wire protocol, port 5439) ---
REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "")
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))
REDSHIFT_DB = os.environ.get("REDSHIFT_DB", f"retail_{ENV}")   # retail_dev / retail_prod
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "admin")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD", "")
REDSHIFT_IAM_ROLE_ARN = os.environ.get("REDSHIFT_IAM_ROLE_ARN", "")   # role Redshift assumes to COPY

# --- Mock retailer API (mock_api/) ---
RETAILER_API_URL = os.environ.get("RETAILER_API_URL", "http://localhost:9100")
RETAILER_API_TOKEN = os.environ.get("RETAILER_API_TOKEN", "local-dev-token")

# --- Automation window: how many weeks every scheduled run restates ---
DEFAULT_LOOKBACK_WEEKS = int(os.environ.get("DEFAULT_LOOKBACK_WEEKS", "5"))


def s3_key(*parts: str) -> str:
    """Build an S3 key inside this environment, e.g. dev/landing/C101/R201/x.csv."""
    return "/".join([S3_ENV_PREFIX, *[p.strip("/") for p in parts]])
