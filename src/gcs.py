import os
import json

from google.cloud import storage


def get_client():
    """Create and return a GCS client."""
    # Create service account credentials from environment variables
    service_account_info = {
        "client_email": os.getenv("CLIENT_EMAIL"),
        "token_uri": os.getenv("TOKEN_URI"),
        "project_id": os.getenv("PROJECT_ID"),
        "private_key": os.getenv("PRIVATE_KEY"),
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
    }
    return storage.Client.from_service_account_info(service_account_info)


def upload_to_gcs(bucket_name, file_name, contents):
    """Upload file contents to GCS bucket."""
    client = get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Always upload as JSON
    if isinstance(contents, dict):
        contents = json.dumps(contents)

    blob.upload_from_string(contents, content_type="application/json")
    print(f"Uploaded {file_name} to {bucket_name}")


def upload_file_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """Upload a local file to GCS."""
    client = get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {destination_blob_name}")


def download_from_gcs(bucket_name, file_name, local_path):
    """Download a file from GCS to local."""
    client = get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(local_path)
    return local_path


def list_files_in_bucket(bucket_name, prefix=None):
    """List all files in a GCS bucket."""
    client = get_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    files = []
    for blob in blobs:
        files.append(blob.name)

    return files
