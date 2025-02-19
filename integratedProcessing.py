import os
import pandas as pd
import pyarrow.parquet as pq
import gc
from logger import get_logger
from helper import save_progress,load_progress,update_status_file
from correctTheData import correct_data_with_mapping
from fetchFromLatLong import transform_row_group
from RedisUtils.redisProcessing import load_or_create_vendors, get_model_mapping, r

logger = get_logger("Integrated Processing")
# BASE_DIR = 'temp_dir'
# PROCESSED_DIR = 'processed_dir'

async def process_data_with_corrections(base_dir,processed_dir,status_file):
    """Process parquet files by first correcting device data and then enriching location details."""
    if not os.path.exists(base_dir):
        logger.error(f"Directory '{base_dir}' does not exist.")
        return
    
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir) 
    
    progress = load_progress()
    last_processed_file = progress.get("file") if progress else None
    last_processed_row = progress.get("row_count", 0) if progress else 0 # Row group count

    logger.info("Starting integrated processing of Parquet files.")

    predefined_vendors = load_or_create_vendors()
    model_mapping = get_model_mapping()
    redis_conn = r  # Redis connection

    status_dict = {}

    for file in os.listdir(base_dir):
        # if file.endswith('.parquet') and file.startswith("cleaned_"):
        if file.endswith('.parquet'):
            file_path = os.path.join(base_dir, file)
            output_file_path = os.path.join(processed_dir, f"{file}")

            logger.info(f"Processing file: {file_path}")
            try:
                with pq.ParquetFile(file_path) as parquet_file:
                    # parquet_file = pq.ParquetFile(file_path)
                    processed_row_group_count = last_processed_row if file_path == last_processed_file else 0
                    for row_group_index in range(parquet_file.num_row_groups):
                        # Read row group
                        table = parquet_file.read_row_group(row_group_index)
                        df_chunk = table.to_pandas()

                        # Step 1: Correct device data
                        corrected_chunk = correct_data_with_mapping(df_chunk, predefined_vendors, model_mapping,r)
                        logger.info("Filled the chunk with make,model from Redis")

                        # Step 2: Fetch and enrich location details
                        transformed_chunk = await transform_row_group(corrected_chunk.to_dict('records'), redis_conn)

                        if transformed_chunk:
                            # save_transformed_row_group(transformed_data, output_file_path) # Save the row group
                            processed_row_group_count += 1
                            save_progress(file_path, processed_row_group_count)
                            logger.info(f"Total row groups processed so far from {file_path}: {processed_row_group_count}")

                        # Convert back to DataFrame
                        transformed_df = pd.DataFrame(transformed_chunk)

                    if os.path.exists(output_file_path):
                        existing_table = pq.read_table(output_file_path)
                        new_table = pq.Table.from_pandas(transformed_df)
                        combined_table = pq.concat_tables([existing_table, new_table])

                        # Write the combined table back to the Parquet file
                        pq.write_table(combined_table, output_file_path, compression='snappy')
                    else:
                        # If the file doesn't exist, write as a new Parquet file
                        transformed_df.to_parquet(output_file_path, engine='pyarrow', compression='snappy', index=False)

                    # Write the final transformed data

                    # transformed_df.to_parquet(
                    #     output_file_path,
                    #     engine='pyarrow',
                    #     compression='snappy',
                    #     index=False,
                    #     append=True if os.path.exists(output_file_path) else False
                    # )

                    del df_chunk, corrected_chunk, transformed_chunk, transformed_df, table
                    gc.collect()
                
                status_dict[file] = "success"
                logger.info(f"Processed file saved at: {output_file_path}")
                update_status_file(status_file, status_dict) 
                os.remove(file_path)
                logger.info(f"Deleted the file: {file_path}")
            
            except Exception as e:
                status_dict[file] = "failed"
                update_status_file(status_file, status_dict) 
                logger.error(f"Error processing file '{file_path}': {e}")

    # update_status_file(status_file, status_dict)
    logger.info("Processing completed for all files.")

# Example usage:
# asyncio.run(process_data_with_corrections('your_base_directory'))
