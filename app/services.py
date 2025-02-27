import boto3
import json
import time
import uuid
import os
import pandas as pd
from app.config import S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY

# Read optional MinIO/S3 local endpoint
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", None)

# Initialize S3 Client with optional MinIO support
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,  # This makes it work with MinIO locally
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def generate_package_id():
    return f"{uuid.uuid4()}"

def upload_to_s3(file_data, package_id, file_type):
    """
    Uploads a file (JSON or TSV) to S3/MinIO.
    - file_data: The actual file content (string for JSON, binary for TSV)
    - package_id: The package name (used for structuring storage)
    - file_type: "json" or "tsv" (or other formats in the future)
    
    Returns the S3 URL of the uploaded file.
    """
    
    # Define correct file extensions and content types
    extensions = {"json": "json", "tsv": "tsv"}
    content_types = {"json": "application/json", "tsv": "text/tab-separated-values"}
    
    # Ensure file type is valid
    if file_type not in extensions:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    # Generate S3 object key (folder structure for organization)
    s3_key = f"{package_id}"
    
    # Convert JSON to bytes if needed
    # if file_type == "json":
    #     file_data = json.dumps(file_data).encode("utf-8")
    if isinstance(file_data, str):  # Convert TSV string to bytes
        file_data = file_data.encode("utf-8")

    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=file_data,
        ContentType=content_types[file_type]
    )

    # Return file URL
    return f"{S3_ENDPOINT_URL}/{S3_BUCKET}/{s3_key}" if S3_ENDPOINT_URL else f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"


def update_package_list(append=False):
    """ Reads existing package metadata from S3 and generates a new package list """
    s3_key = "package_list.json"
    package_list = []

    if append:
        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            package_list = json.loads(obj["Body"].read().decode("utf-8")).get("packages", [])
        except s3_client.exceptions.NoSuchKey:
            package_list = []

    # List all objects in the 'packages/' prefix
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix="packages/")
    for obj in response.get("Contents", []):
        if obj["Key"].endswith("metadata.json"):
            package_id = obj["Key"].split("/")[1]
            try:
                metadata_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=obj["Key"])
                metadata = json.loads(metadata_obj["Body"].read().decode("utf-8"))
                if isinstance(metadata, dict):
                    package_list.append({
                        **metadata
                    })
                else:
                    print(f"Warning: Metadata for package {package_id} is not a dictionary.")
            except s3_client.exceptions.NoSuchKey:
                continue

    # Wrap the package list in the initial JSON structure
    package_list_json = {"packages": package_list}

    # Save the updated package list back to S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(package_list_json),
        ContentType="application/json"
    )

    # Wrap the package list in the initial JSON structure
    package_list_json = {"packages": package_list}

    # Save the updated package list back to S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(package_list_json),
        ContentType="application/json"
    )