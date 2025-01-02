
#window functions


from pyspark.sql import *
from pyspark.sql.functions import *

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"C:\Users\mouni\Desktop\veg_price_data.csv"
df=spark.read.format("csv").option("header","true").load(data)
# df.show()

win2=Window.partitionBy("veg_name").orderBy(col("date"))

# we need  price with previous price, we need lag value with rest of changes in price for each day

df=df.withColumn("previous_price",lag("price").over(win2))

df = df.withColumn("day_change",
    when(col("previous_price").isNull(), lit("0"))
    .when(col("price") > col("previous_price"),
          concat((col("price") - col("previous_price")).cast("int").cast("string"), lit("up")))
    .when(col("price") < col("previous_price"),
          concat((col("previous_price") - col("price")).cast("int").cast("string"), lit("down")))
    .otherwise("0")
)

# we need first price because we are comparing first value with rest of changes in price for each day

df = df.withColumn("starting_price", first("price").over(win2))
df.show()
df = df.withColumn("week_change",
    when(col("starting_price").isNull(), lit("0"))
    .when(col("price") > col("starting_price"),
          concat((col("price") - col("starting_price")).cast("int").cast("string"), lit("up")))
    .when(col("price") < col("starting_price"),
          concat((col("starting_price") - col("price")).cast("int").cast("string"), lit("down")))
    .otherwise("0")
)
df.show()







+--------+----------+-----+--------------+----------+--------------+
|veg_name|      date|price|previous_price|day_change|starting_price|
+--------+----------+-----+--------------+----------+--------------+
| Spinach|01-10-2024|   22|          null|         0|            22|
| Spinach|02-10-2024|   29|            22|       7up|            22|
| Spinach|03-10-2024|   28|            29|     1down|            22|
+--------+----------+-----+--------------+----------+--------------+

+--------+----------+-----+--------------+----------+--------------+-----------+
|veg_name|      date|price|previous_price|day_change|starting_price|week_change|
+--------+----------+-----+--------------+----------+--------------+-----------+
| Spinach|01-10-2024|   22|          null|         0|            22|          0|
| Spinach|02-10-2024|   29|            22|       7up|            22|        7up|
| Spinach|03-10-2024|   28|            29|     1down|            22|        6up|
+--------+----------+-----+--------------+----------+--------------+-----------+


