from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("LeftAntiJoinExample").getOrCreate()

# Create the first dataframe
df1 = spark.createDataFrame([("A", 1), ("B", 2), ("C", 3)], ["letter", "number"])

# Create the second dataframe
df2 = spark.createDataFrame([("A", 4), ("B", 5)], ["letter", "value"])

# Perform the left anti join
left_anti_join = df1.join(df2, df1['letter'] == df2['letter'], "left")

# Show the result of the join
left_anti_join.show()


#left anti vs left join

#left join

+------+------+------+-----+
|letter|number|letter|value|
+------+------+------+-----+
|     B|     2|     B|    5|
|     C|     3|  null| null|
|     A|     1|     A|    4|
+------+------+------+-----+


#left anit gives only --values from left dataframe that does not have matching vlaues in right

+------+------+
|letter|number|
+------+------+
|     C|     3|
+------+------+

#left_semi gives only values from left dataframe that have matching values from right

+------+------+
|letter|number|
+------+------+
|     B|     2|
|     A|     1|
+------+------+
