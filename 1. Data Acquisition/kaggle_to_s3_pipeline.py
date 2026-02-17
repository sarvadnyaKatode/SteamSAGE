''' pip install requests boto3 s3fs
Make sure installed
Before Running

Set Kaggle credentials:
setx KAGGLE_USERNAME "your_username"
setx KAGGLE_KEY "your_new_key"

'''
import os
import requests
import boto3
import s3fs
import zipfile
import shutil
from requests.auth import HTTPBasicAuth

# -------- CONFIGURATION --------
DATASET_OWNER = "crainbramp"
DATASET_NAME = "steam-dataset-2025-multi-modal-gaming-analytics"

S3_BUCKET = "steam-dataset-2025-bucket"
ZIP_FILE_KEY = "Steam.zip"
OUTPUT_FOLDER = "Steam"
# --------------------------------


def download_and_upload():
    """Download dataset from Kaggle and upload to S3"""

    url = f"https://www.kaggle.com/api/v1/datasets/download/{DATASET_OWNER}/{DATASET_NAME}"

    kaggle_user = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")

    if not kaggle_user or not kaggle_key:
        raise Exception("Kaggle credentials not set in environment variables.")

    s3 = boto3.client("s3")

    print("Downloading from Kaggle and streaming to S3...")

    with requests.get(
        url,
        stream=True,
        auth=HTTPBasicAuth(kaggle_user, kaggle_key),
    ) as response:

        response.raise_for_status()
        s3.upload_fileobj(response.raw, S3_BUCKET, ZIP_FILE_KEY)

    print("Upload completed successfully.")


def unzip_inside_s3():
    """Unzip the file directly inside S3"""

    fs = s3fs.S3FileSystem(anon=False)
    zip_path = f"{S3_BUCKET}/{ZIP_FILE_KEY}"

    print(f"Opening ZIP from S3: s3://{zip_path}")

    with fs.open(zip_path, "rb") as f:
        with zipfile.ZipFile(f) as z:

            for filename in z.namelist():

                if filename.endswith("/"):
                    continue

                target_path = f"{S3_BUCKET}/{OUTPUT_FOLDER}/{filename}"

                print(f"Extracting: {filename}")

                with z.open(filename) as source_stream:
                    with fs.open(target_path, "wb") as target_stream:
                        shutil.copyfileobj(source_stream, target_stream)

    print("Extraction completed.")


def delete_zip():
    """Delete ZIP file after extraction"""

    boto3.client("s3").delete_object(Bucket=S3_BUCKET, Key=ZIP_FILE_KEY)
    print("ZIP file deleted from S3.")


if __name__ == "__main__":
    download_and_upload()
    unzip_inside_s3()
    delete_zip()
