from extract import extract
from load import load
from config import INPUT_DATA_PATH, OUTPUT_DATA_PATH

import logging as log
# Configure logging
log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = log.getLogger(__name__)


def main(): 
    df = extract(INPUT_DATA_PATH)
    logger.info(f"DataFrame loaded with {df.count()} rows from {INPUT_DATA_PATH}")
    logger.info(df.show())
    
    load(df, OUTPUT_DATA_PATH, "agr_1251")
    logger.info(f"DataFrame saved to {OUTPUT_DATA_PATH} in Parquet format")
    logger.info("Data processing completed successfully.")


    
if __name__ == "__main__":
     main()
     logger.info("Starting the data processing application...")     