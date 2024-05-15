import boto3
from botocore.client import Config

# Your Storj S3 credentials and endpoint
access_key = 'your_storj_access_key'
secret_key = 'your_storj_secret_key'
endpoint_url = 'https://gateway.storjshare.io'  # or the appropriate Storj S3 gateway URL

# Create a session
session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=endpoint_url,
    config=Config(signature_version='s3v4')
)

# Your bucket and file details
bucket_name = 'your_storj_bucket_name'
file_key = 'arrowlake/response.json'  # Path to your file in the bucket

# Download the file
s3.download_file(bucket_name, file_key, 'response.json')
print("File downloaded successfully.")
