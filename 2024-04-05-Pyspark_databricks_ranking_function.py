# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/empmysql-1.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
win2=Window.orderBy(col("sal").desc())

df21=df.withColumn("hire_date",to_date("hiredate","d-MMM-YYY")).withColumn("drank",dense_rank().over(win2))\
.withColumn("rank",rank().over(win2))\
.withColumn("rownumber",row_number().over(win2))

#rank will skip rank value if htere is duplicate 

df21.show()

# COMMAND ----------

file_location = "/FileStore/tables/INFY.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df3 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df3)

# COMMAND ----------


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

df21A=df3.withColumnRenamed("Adj Close","adjclose")

win21=Window.orderBy(col("Date").desc())

df21B=df21A.withColumn("lead",lead(col("adjclose")).over(win21))

#rank will skip rank value if htere is duplicate 

df21B.show()

# COMMAND ----------


