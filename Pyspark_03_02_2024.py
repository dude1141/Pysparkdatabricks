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
file_location = "/FileStore/tables/10000Records.csv"
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

df1=df
import re
cols=[re.sub("[^A-Za-z0-9]","",x) for x in df1.columns]
df2=df1.toDF(*cols)
df2.printSchema()

from pyspark.sql.functions import regexp_replace

ndf=df2.withColumn("SSN",regexp_replace("SSN","-",""))
# display(ndf)


def offer(wt):
    if wt>10 and wt <=40:
        return "10% discount"
    elif wt >=40 and wt <=60:
        return "10% discount"
    else :
        return "50% of insurance"

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

off1=udf(offer)
ndf1=ndf.withColumn("SSN",regexp_replace("SSN","-","")).withColumn("Offers",off1(col("WeightinKgs")))
# display(ndf1)


from pyspark.sql.types import *
from pyspark.sql.functions import *

# ndf2=ndf1.withColumn("dobts",(concat_ws("",col("DateofBirth"),col("TimeofBirth")).cast(Timestamp)))
# display(ndf2)


# df2 = spark.createDataFrame(data, ["DateofBirth", "TimeofBirth"])
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format

# # Convert dateofbirth to timestamp
# df3 = ndf1.withColumn(
#     "timestamp",
#     to_timestamp(col("TimeofBirth") + " " + col("TimeofBirth"), "yyyy-MM-dd HH:mm:ss a")
# )

# # Set the timestamp to the start of the day
# df4 = df3.withColumn(
#     "converted_date",
#     date_format(col("timestamp").cast("date"), "yyyy-MM-dd") + " 00:00:00"
# )

# # Select the desired columns

df7 = ndf1.withColumn("dobts", to_timestamp(concat(col("DateofBirth"), lit(" "), col("TimeofBirth")), "yyyy-MM-dd hh:mm:ss a"))



# df7=s to_timestamp(concat(col("DateofBirth")).show(False)
                         
df8 = df7.withColumn("dobts", date_format("dobts", "yyyy-MM-dd HH:mm:ss"))


df6 = df7.select("dobts")

display(df6)

# COMMAND ----------

df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])

df.select(to_timestamp(df.t, 'yyyy-MM-dd HH:mm:ss')).show()

df.select(to_timestamp('t')).show()

#convert to unixtimestamp for column t
df.select(to_date('t')).show()

# ('1997-02-28 10:30:00',) ---tuple with singel element
# display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `10000Records_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "10000Records_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
