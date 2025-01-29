from pyspark.sql import *
spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"C:\Users\mouni\Desktop\json2.json"
df=spark.read.format("json").option("multiline","true").option("mode", "DROPMALFORMED").load(data)
df.show()
#extract using col(loc)[0]
# df2=df.withColumn("lang",col("loc")[0]).withColumn("latitude",col("loc")[1])
# df2.show()

{
  "id": 101,
  "user": {
    "name": "Alice",
    "age": 30,
    "address": {
      "city": "Paris",
      "country": "France"
    },
    "phoneNumbers": [12345, 67890]
  },
  "is_active": true
}

df2=df.select(col("user.name"),col("user.address.city"),col("user.phoneNumbers")[0].alias("first_phone"))
df2.show()

df3=df.select(col("user.name"),col("user.address.city"),explode(col("user.phoneNumbers")).alias("first_phone"))
df3.show()



+-----+-----+-----------+
| name| city|first_phone|
+-----+-----+-----------+
|Alice|Paris|      12345|
+-----+-----+-----------+

+---------+-----------------+-----------+
|user.name|user.address.city|first_phone|
+---------+-----------------+-----------+
|    Alice|            Paris|      12345|
|    Alice|            Paris|      67890|
+---------+-----------------+-----------+