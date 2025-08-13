#from config import INPUT_DATA_PATH, OUTPUT_DATA_PATH
from pyspark.sql import SparkSession
import os


import logging as log
# Configure logging
log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = log.getLogger(__name__)


def extract(path):
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataProcessingApp").getOrCreate()
    
    # Read input data
    df = spark.read.csv(f"{path}*.csv", header=True, inferSchema=True)

    for file in os.listdir(path):
        if file.endswith('.csv'):
            temp_df = spark.read.csv(os.path.join(path, file), header=True, inferSchema=True)
            logger.info(f"{file}: {temp_df.count()} rows")
    logger.info(f"Total rows in the DataFrame: {df.count()}")
    
    return df
    # Stop the Spark session
    spark.stop()