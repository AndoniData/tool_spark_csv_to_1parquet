from pyspark.sql import SparkSession
import os

import logging as log
#Configure logging
log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = log.getLogger(__name__)

def load(df, path, name):
    """
    Loads the DataFrame to the specified path in Parquet format.

    Parameters:
    df (DataFrame): The DataFrame to be loaded.
    path (str): The path where the DataFrame will be saved.
    """
    output_path = os.path.join(path, name)
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataProcessingApp").config("spark.executor.memory", "16g").config("spark.driver.memory", "16g").getOrCreate()

    # Write DataFrame to Parquet format
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    
    logger.info(f"DataFrame saved to {output_path} in Parquet format")
    
    # Stop the Spark session
    spark.stop()