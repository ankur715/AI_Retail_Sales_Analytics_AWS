import os
import sys
from pathlib import Path

# Tests never touch real AWS: fake credentials + a fixed bucket name, set
# before etl.config is imported anywhere.
os.environ.update({
    "RETAIL_ENV": "dev",
    "S3_BUCKET": "test-retail-lake",
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_REGION": "us-east-1",
})
os.environ.pop("AWS_PROFILE", None)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
