
# import pyarrow.parquet as pq
# import pandas as pd
# import numpy as np
# import os
# from logger import get_logger

# logger = get_logger("DataCleaning")

# def clean_data(temp_dir):
#     """Clean data in the given files from the temporary directory."""
#     total_discarded_make_model = 0
#     total_discarded_lat_long = 0
#     total_discarded_all_null_ids = 0
#     total_discarded_duplicates = 0

#     # List all Parquet files in the temporary directory
#     for file_name in os.listdir(temp_dir):
#         if file_name.endswith(".parquet"):
#             file_path = os.path.join(temp_dir, file_name)
#             output_file_path = os.path.join(temp_dir, f"cleaned_{file_name}")  # Use a new cleaned file
#             logger.info(f"Processing file: {file_name}")

#             discarded_make_model = 0
#             discarded_lat_long = 0
#             discarded_all_null_ids = 0
#             discarded_duplicates = 0

#             try:
#                 parquet_file = pq.ParquetFile(file_path)
#             except Exception as e:
#                 logger.error(f"Error reading parquet file {file_name}: {e}")
#                 continue

#             cleaned_data_list = []  # Store chunks to avoid memory issues

#             # Process each row group separately
#             for row_group_idx in range(parquet_file.num_row_groups):
#                 logger.info(f"Processing Row Group {row_group_idx + 1}/{parquet_file.num_row_groups} for {file_name}")

#                 # Read a row group as a DataFrame
#                 chunk = parquet_file.read_row_group(row_group_idx).to_pandas()

#                 # Convert specific columns to string to prevent type issues
#                 chunk['device_ifa'] = chunk['device_ifa'].astype(str)
#                 chunk['dpidsha1'] = chunk['dpidsha1'].astype(str)
#                 chunk['dpidmd5'] = chunk['dpidmd5'].astype(str)

#                 # Fill NaN values with empty strings
#                 chunk = chunk.fillna("").astype(str)

#                 # Convert numeric columns safely
#                 chunk['date'] = pd.to_numeric(chunk['date'], errors='coerce')
#                 if 'bid_price' in chunk.columns:
#                     chunk['bid_price'] = pd.to_numeric(chunk['bid_price'], errors='coerce')
#                 if 'bid_req_adv_floor_sum' in chunk.columns:
#                     chunk['bid_req_adv_floor_sum'] = pd.to_numeric(chunk['bid_req_adv_floor_sum'], errors='coerce')
#                 if 'dsp_net_price_sum' in chunk.columns:
#                     chunk['dsp_net_price_sum'] = pd.to_numeric(chunk['dsp_net_price_sum'], errors='coerce')
#                 if 'curr_excg_rate' in chunk.columns:
#                     chunk['curr_excg_rate'] = pd.to_numeric(chunk['curr_excg_rate'], errors='coerce')

#                 # Discard rows with empty make/model
#                 initial_count = len(chunk)
#                 chunk = chunk[~((chunk['device_vendor'] == '') & (chunk['device_model'] == ''))]
#                 discarded_make_model += (initial_count - len(chunk))

#                 # Discard rows with empty latitude/longitude
#                 initial_count = len(chunk)
#                 chunk = chunk[~((chunk['latitude'] == '') & (chunk['longitude'] == ''))]
#                 discarded_lat_long += (initial_count - len(chunk))

#                 # Discard rows with all three empty IDs
#                 initial_count = len(chunk)
#                 chunk = chunk[~((chunk['device_ifa'] == '') & (chunk['dpidsha1'] == '') & (chunk['dpidmd5'] == ''))]
#                 discarded_all_null_ids += (initial_count - len(chunk))

#                 # Replace empty 'device_ifa' with 'dpidsha1' or 'dpidmd5'
#                 chunk['device_ifa'] = np.where(chunk['device_ifa'] != '', chunk['device_ifa'],
#                                                np.where(chunk['dpidsha1'] != '', chunk['dpidsha1'], chunk['dpidmd5']))

#                 # Drop duplicates within the current chunk
#                 initial_count = len(chunk)
#                 chunk = chunk.drop_duplicates()
#                 discarded_duplicates += (initial_count - len(chunk))

#                 cleaned_data_list.append(chunk)  # Store cleaned chunks

#             # Merge all cleaned chunks into a single DataFrame
#             if cleaned_data_list:
#                 final_df = pd.concat(cleaned_data_list, ignore_index=True)

#                 # Write to a new Parquet file
#                 final_df.to_parquet(output_file_path, engine="pyarrow", index=False, compression="snappy")

#                 logger.info(f"Cleaned data saved to {output_file_path}")

#             # Update total discarded row counts
#             total_discarded_make_model += discarded_make_model
#             total_discarded_lat_long += discarded_lat_long
#             total_discarded_all_null_ids += discarded_all_null_ids
#             total_discarded_duplicates += discarded_duplicates

#         # Rename cleaned file back to the original name
#         os.remove(file_path)  # Delete original file
#         os.rename(output_file_path, file_path)  # Rename cleaned file
#         logger.info(f"Replaced original file with cleaned version: {file_path}")    

#     # Log final cleaning summary
#     logger.info("Cleaning Summary:")
#     logger.info(f"Total discarded rows with empty make/model: {total_discarded_make_model}")
#     logger.info(f"Total discarded rows with empty lat/long: {total_discarded_lat_long}")
#     logger.info(f"Total discarded rows with empty deviceId, sha, and md5: {total_discarded_all_null_ids}")
#     logger.info(f"Total discarded duplicate rows: {total_discarded_duplicates}")

import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import os
from logger import get_logger

logger = get_logger("DataCleaning")

def clean_data(temp_dir):
    """Clean data in the given files from the temporary directory."""
    total_discarded_make_model = 0
    total_discarded_lat_long = 0
    total_discarded_all_null_ids = 0
    total_discarded_duplicates = 0

    # List all Parquet files in the temporary directory
    for file_name in os.listdir(temp_dir):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(temp_dir, file_name)
            output_file_path = os.path.join(temp_dir, f"cleaned_{file_name}")  # Use a new cleaned file
            logger.info(f"Processing file: {file_name}")

            discarded_make_model = 0
            discarded_lat_long = 0
            discarded_all_null_ids = 0
            discarded_duplicates = 0

            try:
                with pq.ParquetFile(file_path) as parquet_file:
                    cleaned_data_list = []  # Store chunks to avoid memory issues

                    # Process each row group separately
                    for row_group_idx in range(parquet_file.num_row_groups):
                        logger.info(f"Processing Row Group {row_group_idx + 1}/{parquet_file.num_row_groups} for {file_name}")

                        # Read a row group as an Arrow Table and convert it to a Pandas DataFrame
                        chunk = parquet_file.read_row_group(row_group_idx).to_pandas()

                        # Convert specific columns to string to prevent type issues
                        chunk['device_ifa'] = chunk['device_ifa'].astype(str)
                        chunk['dpidsha1'] = chunk['dpidsha1'].astype(str)
                        chunk['dpidmd5'] = chunk['dpidmd5'].astype(str)

                        # Fill NaN values with empty strings
                        chunk = chunk.fillna("").astype(str)

                        # Convert numeric columns safely
                        numeric_columns = ['date', 'bid_price', 'bid_req_adv_floor_sum', 'dsp_net_price_sum', 'curr_excg_rate']
                        for col in numeric_columns:
                            if col in chunk.columns:
                                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

                        # Discard rows with empty make/model
                        initial_count = len(chunk)
                        chunk = chunk[~((chunk['device_vendor'] == '') & (chunk['device_model'] == ''))]
                        discarded_make_model += (initial_count - len(chunk))

                        # Discard rows with empty latitude/longitude
                        initial_count = len(chunk)
                        chunk = chunk[~((chunk['latitude'] == '') & (chunk['longitude'] == ''))]
                        discarded_lat_long += (initial_count - len(chunk))

                        # Discard rows with all three empty IDs
                        initial_count = len(chunk)
                        chunk = chunk[~((chunk['device_ifa'] == '') & (chunk['dpidsha1'] == '') & (chunk['dpidmd5'] == ''))]
                        discarded_all_null_ids += (initial_count - len(chunk))

                        # Replace empty 'device_ifa' with 'dpidsha1' or 'dpidmd5'
                        chunk['device_ifa'] = np.where(chunk['device_ifa'] != '', chunk['device_ifa'],
                                                    np.where(chunk['dpidsha1'] != '', chunk['dpidsha1'], chunk['dpidmd5']))

                        # Drop duplicates within the current chunk
                        initial_count = len(chunk)
                        chunk = chunk.drop_duplicates()
                        discarded_duplicates += (initial_count - len(chunk))

                        cleaned_data_list.append(chunk)  # Store cleaned chunks

                    # Merge all cleaned chunks into a single DataFrame
                    if cleaned_data_list:
                        final_df = pd.concat(cleaned_data_list, ignore_index=True)

                        # Write to a new Parquet file
                        final_df.to_parquet(output_file_path, engine="pyarrow", index=False, compression="snappy")

                        logger.info(f"Cleaned data saved to {output_file_path}")

                    # Update total discarded row counts
                    total_discarded_make_model += discarded_make_model
                    total_discarded_lat_long += discarded_lat_long
                    total_discarded_all_null_ids += discarded_all_null_ids
                    total_discarded_duplicates += discarded_duplicates

                # Rename cleaned file back to the original name
                os.remove(file_path)  # Delete original file
                os.rename(output_file_path, file_path)  # Rename cleaned file
                logger.info(f"Replaced original file with cleaned version: {file_path}")

            except Exception as e:
                logger.error(f"Error processing parquet file {file_name}: {e}")
                continue

    # Log final cleaning summary
    logger.info("Cleaning Summary:")
    logger.info(f"Total discarded rows with empty make/model: {total_discarded_make_model}")
    logger.info(f"Total discarded rows with empty lat/long: {total_discarded_lat_long}")
    logger.info(f"Total discarded rows with empty deviceId, sha, and md5: {total_discarded_all_null_ids}")
    logger.info(f"Total discarded duplicate rows: {total_discarded_duplicates}")
