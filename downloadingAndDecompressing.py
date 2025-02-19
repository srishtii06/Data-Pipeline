import os
import tempfile
import zstandard as zstd
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from logger import get_logger
from config import config

ACCESS_KEY = config.ACCESS_KEY_TEST
SECRET_KEY = config.SECRET_KEY_TEST
ENDPOINT_URL = config.ENDPOINT_URL_TEST
REGION_NAME = config.REGION_NAME_TEST
BUCKET_NAME = config.BUCKET_NAME_TEST

logger=get_logger("DownloadingAndDecompressing")

def listFilesInBucket(bucket_name, date_filter=None):
    """List files in the specified bucket, only including files with 'bid' or 'nobid' in their names."""
    try:
        s3_client = boto3.client(
            "s3",
            region_name=REGION_NAME,
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
        )
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if "Contents" in response:
            files = response["Contents"]
            filtered_files = []

            print("Files in S3 Bucket:")
            for item in files:
                file_name = item["Key"]
                file_size = item["Size"] / 1024  # Convert size to KB
                
                # Include files with 'bid' or 'nobid' in their names
                if "bid" in file_name.lower() or "nobid" in file_name.lower():
                    if date_filter:
                        if date_filter in file_name:
                            filtered_files.append((file_name, file_size))
                    else:
                        filtered_files.append((file_name, file_size))

            for file in filtered_files:
                print(f" {file[0]} - {file[1]/1024:.2f} KB")

            return filtered_files
        else:
            logger.info("No files found in the bucket.")
            return []
    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f"Error: {e}")
        return []



def downloadAndDecompressFiles(files):
    """Download and decompress files in a temp directory inside the current folder."""
    s3_client = boto3.client(
        "s3",
        region_name=REGION_NAME,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    # Create a "temp" directory inside the current working directory
    temp_dir = os.path.join(os.getcwd(), "temp")
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"Using temporary directory: {temp_dir}")

    for file_name, _ in files:
        temp_file_path = os.path.join(temp_dir, os.path.basename(file_name))

        try:
            # Download the file
            s3_client.download_file(BUCKET_NAME, file_name, temp_file_path)
            logger.info(f"Downloaded: {file_name} to {temp_file_path}")

            # Decompress if the file is .zst
            if temp_file_path.endswith('.zst'):
                decompressed_file_path = temp_file_path[:-4]  # Remove .zst extension
                try:
                    with open(temp_file_path, 'rb') as compressed_file, open(decompressed_file_path, 'wb') as output_file:
                        dctx = zstd.ZstdDecompressor()
                        with dctx.stream_reader(compressed_file) as reader:
                            while True:
                                chunk = reader.read(65536)  # 64KB chunks
                                if not chunk:
                                    break
                                output_file.write(chunk)

                    logger.info(f"Decompressed: {temp_file_path} to {decompressed_file_path}")

                    # Replace the compressed file with the decompressed one
                    os.remove(temp_file_path)

                    logger.info(f"Replaced compressed file with decompressed file: {decompressed_file_path}")
                except Exception as e:
                    logger.error(f"Error decompressing {file_name}: {e}")

        except Exception as e:
            logger.error(f"Failed to download {file_name}: {e}")