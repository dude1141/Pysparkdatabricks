from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
confluent_key=r"VNH3POCG2IYDVGYP"
confluent_secret = r"cfltkmqEdneGzGwJJgJTmAYwqGG5ITHnRiXRN1w3mY+yCizLz5xVvYXQMPNDD97A"
topic="nov27topic"
broker="pkc-921jm.us-east-2.aws.confluent.cloud:9092"
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("subscribe", topic) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f"""org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluent_key}" password="{confluent_secret}";""") \
    .load()
df=df.selectExpr("CAST(value AS STRING)")

#df.writeStream.outputMode("append").format("console").start().awaitTermination()
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
    df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "nov27table21").save()
    pass
#res.writeStream.foreachBatch(export_to_snow).start().awaitTermination()
# For one-time batch processing, use once=True
res.writeStream \
    .foreachBatch(export_to_snow) \
    .trigger(once=True) \
    .start() \
    .awaitTermination()

# Or, omit .trigger() for default micro-batch mode
res.writeStream \
    .foreachBatch(export_to_snow) \
    .start() \
    .awaitTermination()