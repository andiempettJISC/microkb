import os
import json
import hashlib
import pandas as pd
from flask import Blueprint, request, jsonify
from app.services import generate_package_id, upload_to_s3, update_package_list, s3_client
from app.validations import validate_json
from app.config import S3_BUCKET, AWS_REGION

routes = Blueprint("routes", __name__)

import hashlib

def calculate_checksum(file_obj):
    """ Computes the MD5 hash of a file object """
    md5_hash = hashlib.md5()
    file_obj.seek(0)  # Ensure we start reading from the beginning

    # Read file in chunks to avoid memory issues with large files
    while chunk := file_obj.read(8192):  
        md5_hash.update(chunk)

    file_obj.seek(0)  # Reset pointer for further operations
    return md5_hash.hexdigest()

def calculate_checksum_from_body(body):
    """ Calculate the MD5 checksum from a stream body """
    md5 = hashlib.md5()
    body_content = body.read()
    md5.update(body_content)
    return md5.hexdigest()


def allowed_file(filename):
    """Check if file type is allowed."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in {"tsv", "csv"}


@routes.route("/upload", methods=["POST"])
def upload_package():
    """ Handles TSV ingestion, converts to JSON, and updates package index """
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    package_name = request.form.get("package_name")

    if file.filename == "":
        return jsonify({"error": "No filename"}), 400
    if not allowed_file(file.filename):
        return jsonify({"error": "Invalid file type"}), 400
    if file.stream.read(1) == b'':
        return jsonify({"error": "Empty file provided"}), 400
    file.stream.seek(0)  # Reset the stream position to the beginning

    if not package_name:
        return jsonify({"error": "Package name required"}), 400

    # Generate or use provided package ID
    package_id = request.form.get("package_id") or generate_package_id()
    
    # Read TSV and convert to JSON
    try:
        df = pd.read_csv(file, sep="\t")
    except Exception as e:
        return jsonify({"error": f"Error reading TSV: {e}"}), 400
    try:
        json_data = df.to_json(orient="records")
    except Exception as e:
        return jsonify({"error": f"Error converting to JSON: {e}"}), 400

    # Validate JSON data
    is_valid, validation_errors, validation_warnings = validate_json(json.loads(json_data))
    print(is_valid, validation_errors, validation_warnings)
    if not is_valid:
        return jsonify({"error": "Validation failed", "errors": json.loads(validation_errors), "warnings": json.loads(validation_warnings)}), 400

    # Calculate title count using pandas
    title_count = len(df)

    # Calculate checksum of the new file
    new_checksum = calculate_checksum(file)
    
    # Check if package exists and get the latest version
    version = 1
    date_created = None
    last_updated = None
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"packages/{package_id}/versions/")
        print(response)
        if "Contents" in response:
            for obj in response["Contents"]:
                print(obj["Key"].split("/")[3])
            latest_version = max(int(obj["Key"].split("/")[3]) for obj in response["Contents"])
            latest_metadata_key = f"packages/{package_id}/versions/{latest_version}/raw.tsv"
            latest_metadata_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=latest_metadata_key)
            latest_checksum = calculate_checksum_from_body(latest_metadata_obj["Body"])
            
            print(f"New checksum: {new_checksum}, Latest checksum: {latest_checksum}")
            if new_checksum != latest_checksum:
                version = latest_version + 1
            else:
                version = latest_version
            # Find the earliest timestamp for date_created
            first_version_obj = min(response["Contents"], key=lambda x: x["LastModified"])
            date_created = first_version_obj["LastModified"].isoformat()
    except s3_client.exceptions.NoSuchKey:
        pass

    # Define S3 paths
    metadata_s3_key = f"packages/{package_id}/metadata.json"
    json_s3_key = f"packages/{package_id}/versions/{version}/data.json"
    tsv_s3_key = f"packages/{package_id}/versions/{version}/raw.tsv"

    # Upload files
    json_url = upload_to_s3(json_data, json_s3_key, 'json')
    file.seek(0)  # Reset file pointer
    tsv_url = upload_to_s3(file, tsv_s3_key, 'tsv')

    # If first upload, Find the earliest timestamp for date_created
    if not date_created:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"packages/{package_id}/versions/")
        if "Contents" in response and response["Contents"]:
            first_version_obj = min(response["Contents"], key=lambda x: x["LastModified"])
            date_created = first_version_obj["LastModified"].isoformat()

    # Use head_object to get the latest version's LastModified timestamp
    latest_version_key = f"packages/{package_id}/versions/{version}/raw.tsv"
    latest_version_obj = s3_client.head_object(Bucket=S3_BUCKET, Key=latest_version_key)
    last_updated = latest_version_obj["LastModified"].isoformat()

    # Package metadata
    # metadata = {
    #     "package_id": package_id,
    #     "package_name": package_name,
    #     "latest_version": version,
    #     "date_created": date_created,
    #     "last_updated": last_updated,
    #     "title_count": title_count,
    #     "versions": {}
    # }
    metadata = {
        "identifier": package_id,
        "name": package_name,
        "latest": version,
        "dateCreated": date_created,
        "lastUpdated": last_updated,
        "titleCount": title_count,
        "versions": {},
        "packageContentAsJson" : f"{request.host_url}package/{package_id}"
    }

     # List all versions and update metadata
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"packages/{package_id}/versions/")
    if "Contents" in response:
        for obj in response["Contents"]:
            version_number = int(obj["Key"].split("/")[3])
            if obj["Key"].endswith("data.json"):
                json_url = f"s3://{S3_BUCKET}/{obj['Key']}"
            elif obj["Key"].endswith("raw.tsv"):
                tsv_url = f"s3://{S3_BUCKET}/{obj['Key']}"
                metadata["versions"][version_number] = {"json": json_url, "tsv": tsv_url}

    # Upload metadata
    upload_to_s3(json.dumps(metadata).encode("utf-8"), metadata_s3_key, 'json')

    update_package_list()

    if validation_warnings:
        return jsonify({"message": "Package uploaded with warnings", "package_id": package_id, "version": version, "warnings": json.loads(validation_warnings)}), 200
    return jsonify({"message": "Package uploaded successfully", "package_id": package_id, "version": version}), 200

@routes.route("/packages", methods=["GET"])
def list_packages():
    """ Returns the package list from S3 """
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key="package_list.json")
        package_list = json.loads(obj["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        package_list = []

    return jsonify(package_list), 200

@routes.route("/package/<package_id>", methods=["GET"])
def get_package(package_id):
    """ Returns metadata for a given package combined with the package JSON from the data.json file """
    try:
        # Fetch metadata
        metadata_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"packages/{package_id}/metadata.json")
        metadata = json.loads(metadata_obj["Body"].read().decode("utf-8"))

        # Fetch package JSON from the latest version
        versions = metadata.get("versions", {})
        if not versions:
            return jsonify({"error": "No versions found"}), 404

        latest_version = max(int(v) for v in versions.keys())
        package_json_key = f"packages/{package_id}/versions/{latest_version}/data.json"
        package_json_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=package_json_key)
        package_json = json.loads(package_json_obj["Body"].read().decode("utf-8"))

        # Combine metadata and package JSON
        # combined_data = {
        #     "metadata": metadata,
        #     "title_list": package_json
        # }
        combined_data = {
        "Packages": [
                {
                    **metadata,
                    "TitleList": package_json
                }
            ]
        }

    except s3_client.exceptions.NoSuchKey:
        return jsonify({"error": "Package not found"}), 404

    return jsonify(combined_data), 200

@routes.route("/package/<package_id>/versions", methods=["GET"])
def list_package_versions(package_id):
    """ Lists all versions of a package """
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"packages/{package_id}/metadata.json")
        metadata = json.loads(obj["Body"].read().decode("utf-8"))
        versions = metadata.get("versions", {})

        if not versions:
            return jsonify({"error": "No versions found"}), 404

    except s3_client.exceptions.NoSuchKey:
        return jsonify({"error": "Package not found"}), 404

    return jsonify({"package_id": package_id, "versions": list(versions.keys())}), 200

@routes.route("/package/<package_id>/version/<int:version>", methods=["GET"])
def get_package_version(package_id, version):
    """ Returns a specific version's metadata """
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"packages/{package_id}/metadata.json")
        metadata = json.loads(obj["Body"].read().decode("utf-8"))
        version_data = metadata["versions"].get(str(version))

        if not version_data:
            return jsonify({"error": "Version not found"}), 404

    except s3_client.exceptions.NoSuchKey:
        return jsonify({"error": "Package not found"}), 404

    return jsonify(version_data), 200

@routes.route("/package/<package_id>/download", methods=["GET"])
def download_tsv(package_id):
    """ Generates a pre-signed URL to download a TSV file """
    version = request.args.get("version")

    try:
        if version:
            # Download specific version
            tsv_key = f"packages/{package_id}/versions/{version}/raw.tsv"
        else:
            # Download latest version
            metadata_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"packages/{package_id}/metadata.json")
            metadata = json.loads(metadata_obj["Body"].read().decode("utf-8"))
            versions = metadata.get("versions", {})
            if not versions:
                return jsonify({"error": "No versions found"}), 404

            latest_version = max(int(v) for v in versions.keys())
            tsv_key = f"packages/{package_id}/versions/{latest_version}/raw.tsv"

        # Get the package name from metadata
        package_name = metadata.get("package_name", package_id)

        # Generate pre-signed URL
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET, 'Key': tsv_key},
            ExpiresIn=3600
        )

        # Set the Content-Disposition header to specify the filename
        response = jsonify({"url": presigned_url, "package_name": package_name})
        # response.headers["Content-Disposition"] = f"attachment; filename={package_name}.tsv"
        return response

    except s3_client.exceptions.NoSuchKey:
        return jsonify({"error": "Package not found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@routes.route("/package/<package_id>", methods=["DELETE"])
def delete_package(package_id):
    """ Deletes a package and all its associated files from S3 """
    try:
        # List all objects in the package directory
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"packages/{package_id}/")
        objects_to_delete = [{"Key": obj["Key"]} for obj in response.get("Contents", [])]

        if not objects_to_delete:
            return jsonify({"error": "Package not found"}), 404

        # Delete all objects in the package directory
        s3_client.delete_objects(
            Bucket=S3_BUCKET,
            Delete={"Objects": objects_to_delete}
        )

        update_package_list()
        
        return jsonify({"success": f"Package {package_id} deleted successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500