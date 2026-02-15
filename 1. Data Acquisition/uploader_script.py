import requests
import boto3
import os

# --- CONFIGURATION ---
KAGGLE_USER = 'omsaialladwar'
KAGGLE_KEY = 'KGAT_634e21b7700cceb9ca4662bae5622850'
DATASET_OWNER = 'crainbramp'   # e.g., 'netflix-inc'
DATASET_NAME = 'steam-dataset-2025-multi-modal-gaming-analytics'  # e.g., 'netflix-prize-data'
S3_BUCKET = 'steam-dataset-2025-bucket'
S3_FILE_NAME = 'Steam.zip'   # What you want to call it in S3
# ---------------------

def stream_kaggle_to_s3():
    # 1. Prepare Kaggle API URL (Standard Kaggle API endpoint)
    url = f"https://www.kaggle.com/api/v1/datasets/download/{DATASET_OWNER}/{DATASET_NAME}"
    
    # 2. Setup AWS S3 Client
    s3 = boto3.client('s3')

    print(f"Starting stream: Kaggle ({DATASET_NAME}) -> EC2 RAM -> S3 ({S3_BUCKET})...")

    # 3. Create the request stream (Does not download immediately)
    with requests.get(url, stream=True, auth=(KAGGLE_USER, KAGGLE_KEY)) as r:
        r.raise_for_status()
        
        # 4. Upload to S3 using the raw stream
        # This reads the stream in chunks and uploads concurrently
        s3.upload_fileobj(
            r.raw, 
            S3_BUCKET, 
            S3_FILE_NAME
        )
        
    print("Success! File uploaded to S3 without touching EC2 disk.")

if __name__ == "__main__":
    stream_kaggle_to_s3()