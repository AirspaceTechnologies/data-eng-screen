import os
from google.cloud import storage
from utils.logging_config import logger


def client():
    return storage.Client.from_service_account_info(
        {
            "client_email": os.getenv("CLIENT_EMAIL"),
            "token_uri": os.getenv("TOKEN_URI"),
            "project_id": os.getenv("PROJECT_ID"),
            "private_key": os.getenv("PRIVATE_KEY").replace(
                "\\n", "\n"
            ),  # mangled by env file
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        }
    )


def upload_to_gcs(bucket_name, file_name, contents):
    client_instance = client()
    bucket = client_instance.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(contents, content_type="application/json")


def download_from_gcs(bucket_name, file_name, local_path):
    """Download a file from GCS to local."""
    client_instance = client()
    bucket = client_instance.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(local_path)
    logger.info(f"Downloaded {file_name} to {local_path}")
