from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
from pyspark.sql.functions import *

lines = spark.readStream.format("socket").option("host", "20.9.131.30").option("port", 1234).load()
res = (
    lines
    .withColumn("name", split(col("value"), ",")[0])
    .withColumn("age", split(col("value"), ",")[1].cast("int"))
    .withColumn("city", split(col("value"), ",")[2])
).drop("value")

#res.writeStream.outputMode("append").format("console").start().awaitTermination()

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
def export_to_snow(df, epoch_id):
    # Transform and write batchDF
    df.write.mode("overwrite").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "livetab").save()
    pass

res.writeStream.foreachBatch(export_to_snow).start().awaitTermination()

#res.write.mode("append").format("delta").saveAsTable("streamingdata")

