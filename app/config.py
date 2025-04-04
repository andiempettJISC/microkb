import os
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
ADDITIONAL_IDENTIFIERS_ALLOW = os.getenv("ADDITIONAL_IDENTIFIERS_ALLOW").split(',') if os.getenv("ADDITIONAL_IDENTIFIERS_ALLOW") else []
