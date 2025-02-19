from downloadingAndDecompressing import listFilesInBucket, downloadAndDecompressFiles
from dataCleaning import clean_data
from processingData import process_parquet_files
from integratedProcessing import process_data_with_corrections
import asyncio

temp_dir = 'temp'
processed_dir = 'processed_dir'
status_file="status.json"


if __name__ == "__main__":
    date_filter = "2024-02-27"
    bucket_name = "test-es-backup"

    # Step 1: List and filter files in the bucket
    filtered_files = listFilesInBucket(bucket_name, date_filter)
    
    if filtered_files:
         # Step 2: Download and decompress files in temporary directory
        downloadAndDecompressFiles(filtered_files)

        # Step 3: Clean the downloaded files in the same temporary directory
        clean_data(temp_dir)
        # clean_data("SampleData")

        # Step 4: Make Json in Redis using the data 
        # process_parquet_files()

        #Step 5:Fill in the empty values of make from redis and processed data from lat,long
        asyncio.run(process_data_with_corrections(temp_dir,processed_dir,status_file))
  


    else:
        print("No files found for the specified date.")

# clean_data("SampleData")

# asyncio.run(process_data_with_corrections("SampleData","processed_dir","status.json"))