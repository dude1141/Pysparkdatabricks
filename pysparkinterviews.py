from pyarrow import null

from pyspark.sql.functions import split,col
from pyspark.sql import SparkSession



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("FullOuterJoinExample").getOrCreate()

data1= [ (101, 'John'), (102,'Mark'), (103,'KUN')]

data2=[ (102,'Dallkas'),(103,'Newyork'), (104,'Ark')]

schemas=StructType([StructField("empid", StringType()),
                   StructField("name", StringType())])

schemas2=StructType([StructField("empid", StringType()),
                   StructField("city", StringType())])

df1=spark.createDataFrame(data1,schema=schemas)
df2=spark.createDataFrame(data2,schema=schemas2)

df3=df1.alias("a").join(df2.alias("b"),on="empid",how="full_outer")

# df3.show()


df31=df1.alias("a").join(df2.alias("b"),col("a.empid")==col("b.empid"),how="full_outer")
# df31.show()


df4=df31.select(coalesce(col("a.empid"),col("b.empid")).alias("empid"),col("name"),col("city"))
# df4.show()
#
# df_filtered = df31.filter(
#     coalesce(col("df1.empid"), col("df2.empid")).isNotNull()
# )


df32=df1.alias("a").join(df2.alias("b"),on="empid",how="full_outer")
df32.show()

leftsemi=df1.alias("a").join(df2.alias("b"),col("a.empid")==col("b.empid"),how="left_semi")
leftsemi.show()

leftatis=df1.alias("a").join(df2.alias("b"),col("a.empid")==col("b.empid"),how="left_anti")
leftatis.show()
