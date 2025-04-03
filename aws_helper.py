import boto3
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')

# AWS S3 Configuration
S3_BUCKET_NAME = "sensorcase"
ERROR_S3_PREFIX = "sensors/error/"
STABLE_S3_PREFIX = "sensors/stable/"

# Initialize Boto3 S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

upload_dir = "data/upload"
last_uploaded_timestamp = "-"

def upload():
    global last_uploaded_timestamp
    print(f"\x1b[96mLast upload timestamp: {last_uploaded_timestamp}\x1b[0m")

    for file in os.listdir(upload_dir) :
        file_path = os.path.join(upload_dir, file)
        last_uploaded_timestamp = file.split("_")[1]
        prefix = file.split("_")[0]

        if prefix.lower() == "error" :
            upload_to_s3(file_path, "sensors/error/")
            os.remove(file_path)

        if prefix.lower() == "stable" :
            upload_to_s3(file_path, "sensors/stable/")
            os.remove(file_path)

def upload_to_s3(local_file, s3_prefix):
    """Uploads a file to AWS S3."""
    filename = os.path.basename(local_file)
    s3_key = f"{s3_prefix}{filename}"

    try:
        s3_client.upload_file(local_file, S3_BUCKET_NAME, s3_key)
        print(f"\x1b[94mUploaded {filename} to s3://{S3_BUCKET_NAME}/{s3_key}\x1b[0m")
    except Exception as e:
        print(f"\x1b[91mERROR: Failed to upload {filename} to S3 - {str(e)}\x1b[0m")
