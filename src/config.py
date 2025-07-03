import os
from dotenv import load_dotenv

#loading variables
load_dotenv("env/paths.env")

#accesing variables
INPUT_DATA_PATH = os.getenv("INPUT_DATA_PATH")
OUTPUT_DATA_PATH = os.getenv("OUTPUT_DATA_PATH")
SCHEMA_JSON_PATH = os.getenv("SCHEMA_JSON_PATH")


