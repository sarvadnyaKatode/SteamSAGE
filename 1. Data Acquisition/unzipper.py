import s3fs
import zipfile
import shutil

# --- CONFIGURATION ---
BUCKET = 'steam-dataset-2025-bucket'
ZIP_FILE_KEY = 'Steam.zip'       # The file you uploaded in Step 1
OUTPUT_FOLDER = 'Steam'   # Where the 4 folders will go
# ---------------------

def unzip_remote_s3():
    # Connect to S3 as if it were a local file system
    fs = s3fs.S3FileSystem(anon=False) # Uses your AWS credentials automatically

    zip_path = f"{BUCKET}/{ZIP_FILE_KEY}"
    
    print(f"Opening remote zip file: {zip_path}...")
    
    # Open the Zip file directly from S3
    with fs.open(zip_path, 'rb') as f:
        with zipfile.ZipFile(f) as z:
            
            # Loop through every file inside the zip
            for filename in z.namelist():
                # Skip directories, we only care about files
                if filename.endswith('/'): 
                    continue
                
                source_path = filename
                target_path = f"{BUCKET}/{OUTPUT_FOLDER}/{filename}"
                
                print(f"Extracting: {filename} -> s3://{target_path}")
                
                # Stream the specific file from the Zip (in S3) to the destination (in S3)
                # This uses RAM buffer, not Disk
                with z.open(source_path) as source_stream:
                    with fs.open(target_path, 'wb') as target_stream:
                        shutil.copyfileobj(source_stream, target_stream)

    print("Extraction Complete!")

if __name__ == "__main__":
    unzip_remote_s3()