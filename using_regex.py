from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Create SparkSession
spark = SparkSession.builder.appName("LocationTransformation").getOrCreate()

# Sample data
data = [("MAHESH", "DIV-CHN_2000", 5000),
        ("ARUN", "DIV-HYD_3000", 4500),
        ("SOUMI", "DIV-DEL_4500", 9000)]

# Define schema and create DataFrame
columns = ["Name", "LOC", "Salary"]
df = spark.createDataFrame(data, columns)

# Extract city abbreviation from LOC using regex
df = df.withColumn("City_Abbr", regexp_extract(col("LOC"), "DIV-(\\w+)_\\d+", 1))

#
# "DIV-(\\w+)-\\d+"
#
# DIV-:
# Matches the literal text DIV- at the start of the string.
# (\\w+):
# \\w+ matches one or more word characters (letters, digits, or underscores).
# The parentheses () around \\w+ create a capturing group to extract the matched part (e.g., CHN, HYD, or DEL).
# -\\d+:
# - matches the literal hyphen -.
# \\d+ matches one or more digits (e.g., 2000, 3000, 4500).

print("df.......")
df.show()
#
# +------+------------+------+---------+
# |  Name|         LOC|Salary|City_Abbr|
# +------+------------+------+---------+
# |MAHESH|DIV-CHN-2000|  5000|      CHN|
# |  ARUN|DIV-HYD-3000|  4500|      HYD|
# | SOUMI|DIV-DEL-4500|  9000|      DEL|
# +------+------------+------+---------+

# Map abbreviations to full city names
df_transformed = df.withColumn(
    "newloc", when (df.City_Abbr == "CHN","CHENNAI")
             .when (df.City_Abbr == "HYD","HYDERABAD")
            .when (df.City_Abbr == "DEL","DELHI")
           .otherwise(df.City_Abbr))


# Drop intermediate column if not needed
df_transformed = df_transformed.drop("City_Abbr")

# Show the transformed DataFrame
df_transformed.show()
#
# +------+------------+------+---------+
# |  Name|         LOC|Salary|   newloc|
# +------+------------+------+---------+
# |MAHESH|DIV-CHN-2000|  5000|  CHENNAI|
# |  ARUN|DIV-HYD-3000|  4500|HYDERABAD|
# | SOUMI|DIV-DEL-4500|  9000|    DELHI|
# +------+------------+------+---------+
