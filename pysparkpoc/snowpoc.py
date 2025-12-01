from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import os


spark = SparkSession.builder.appName("csvdatapoc").master("local").getOrCreate()
path=r"C:\bigdata\drivers"

# Set options below
sfOptions = {
  "sfURL" : "snlvajq-by25357.snowflakecomputing.com",
  "sfUser" : "lakshmidevagiri8",
  "sfPassword" : "January-01-2000",
  "sfDatabase" : "lakshmi",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH",
  "sfRole" : "Accountadmin"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
# Define the folder path
folder_path = "C:\\bigdata\\drivers"

# List all files in the directory
all_files = os.listdir(folder_path)

# Filter for CSV files
csv_files = [f for f in all_files if f.endswith(".csv")]


# Iterate through each CSV file
for csv_file in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    print(f"Reading file: {file_path}")
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    print(f"DataFrame for {csv_file}:")
    tb = os.path.splitext(csv_file)[0]
    tab=re.sub(r'[^a-zA-Z0-9]', '', tb)


    print(f"table name to export: {tab}")
    df.show()
    df.write.mode("overwrite").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable",tab).save()