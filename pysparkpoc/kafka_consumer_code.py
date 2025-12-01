'''from kafka import *
consumer = KafkaConsumer('nov20')
for msg in consumer:
    print (msg.value)'''
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
from pyspark.sql.functions import *
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "nov22") \
  .load()
# df.show(truncate=False)
# df.writeStream.outputMode("append").format("console").start().awaitTermination()
# exit()
df=df.selectExpr("CAST(value AS STRING)")
# res=df
res = (
    df
    .withColumn("name", split(col("value"), ",")[0])
    .withColumn("age", split(col("value"), ",")[1].cast("int"))
    .withColumn("city", split(col("value"), ",")[2])
).drop("value")

# res.writeStream.outputMode("append").format("console").start().awaitTermination()
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
    df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "newkafkalive").save()
    pass
res.writeStream.foreachBatch(export_to_snow).start().awaitTermination()

