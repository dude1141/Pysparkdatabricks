import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType

spark = SparkSession.builder.appName("dev_kafka_nifi_json").master("local[*]").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nov22") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as value")

user_schema = StructType([
    StructField("gender", StringType()),
    StructField("name", StructType([
        StructField("title", StringType()),
        StructField("first", StringType()),
        StructField("last", StringType())
    ])),
    StructField("location", StructType([
        StructField("street", StructType([
            StructField("number", IntegerType()),
            StructField("name", StringType())
        ])),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("postcode", StringType()),
        StructField("coordinates", StructType([
            StructField("latitude", StringType()),
            StructField("longitude", StringType())
        ])),
        StructField("timezone", StructType([
            StructField("offset", StringType()),
            StructField("description", StringType())
        ]))
    ])),
    StructField("email", StringType()),
    StructField("login", StructType([
        StructField("uuid", StringType()),
        StructField("username", StringType()),
        StructField("password", StringType()),
        StructField("md5", StringType()),
        StructField("sha1", StringType()),
        StructField("sha256", StringType())
    ])),
    StructField("dob", StructType([
        StructField("date", StringType()),
        StructField("age", IntegerType())
    ])),
    StructField("registered", StructType([
        StructField("date", StringType()),
        StructField("age", IntegerType())
    ])),
    StructField("phone", StringType()),
    StructField("cell", StringType()),
    StructField("id", StructType([
        StructField("name", StringType()),
        StructField("value", StringType())
    ])),
    StructField("picture", StructType([
        StructField("large", StringType()),
        StructField("medium", StringType()),
        StructField("thumbnail", StringType())
    ])),
    StructField("nat", StringType())
])

schema = StructType([
    StructField("results", ArrayType(user_schema))
])

parsed_df = df.withColumn("json_data", from_json(col("value"), schema))
exploded_df = parsed_df.select(explode(col("json_data.results")).alias("user"))

dim_user1 = exploded_df.select(
    col("user.gender"),
    col("user.name.title").alias("title"),
    col("user.name.first").alias("first_name"),
    col("user.name.last").alias("last_name"),
    col("user.dob.date").alias("dob_date"),
    col("user.dob.age").alias("dob_age"),
    col("user.registered.date").alias("registered_date"),
    col("user.registered.age").alias("registered_age"),
    col("user.email"),
    col("user.phone"),
    col("user.cell"),
    col("user.nat")
)

dim_location1 = exploded_df.select(
    col("user.location.street.number").alias("street_number"),
    col("user.location.street.name").alias("street_name"),
    col("user.location.city"),
    col("user.location.state"),
    col("user.location.country"),
    col("user.location.postcode"),
    col("user.location.coordinates.latitude").alias("latitude"),
    col("user.location.coordinates.longitude").alias("longitude"),
    col("user.location.timezone.offset").alias("tz_offset"),
    col("user.location.timezone.description").alias("tz_description")
)

dim_login1 = exploded_df.select(
    col("user.login.uuid"),
    col("user.login.username"),
    col("user.login.password"),
    col("user.login.md5"),
    col("user.login.sha1"),
    col("user.login.sha256"),
)

dim_id1 = exploded_df.select(
    col("user.id.name").alias("id_name"),
    col("user.id.value").alias("id_value")
)

dim_picture1 = exploded_df.select(
    col("user.picture.large").alias("pic_large"),
    col("user.picture.medium").alias("pic_medium"),
    col("user.picture.thumbnail").alias("pic_thumbnail")
)

# Example fact table: combine selected columns from dimensions
fact_user1 = exploded_df.select(
    col("user.email").alias("user_email"),
    col("user.location.city").alias("user_city"),
    col("user.login.uuid").alias("login_uuid"),
    col("user.id.value").alias("id_value"),
    col("user.picture.large").alias("picture_large")
)

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

def export_to_snowflake(table_name):
    def export(df, epoch_id):
        df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", table_name).save()
    return export

# Write each dimension and fact table to Snowflake
dim_user1.writeStream.foreachBatch(export_to_snowflake("dim_user12")).start()
dim_location1.writeStream.foreachBatch(export_to_snowflake("dim_location12")).start()
dim_login1.writeStream.foreachBatch(export_to_snowflake("dim_login12")).start()
dim_id1.writeStream.foreachBatch(export_to_snowflake("dim_id12")).start()
dim_picture1.writeStream.foreachBatch(export_to_snowflake("dim_picture12")).start()
fact_user1.writeStream.foreachBatch(export_to_snowflake("fact_user12")).start()

# Await termination for all streams
spark.streams.awaitAnyTermination()