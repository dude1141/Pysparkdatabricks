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


leftouter
+-----+----+-------+
|empid|name|   city|
+-----+----+-------+
|  101|John|   NULL|
|  102|Mark|Dallkas|
|  103| KUN|Newyork|
+-----+----+-------+

leftsemi same as leftouter, but it returns only matched rows and columns from left table
+-----+----+
|empid|name|
+-----+----+
|  102|Mark|
|  103| KUN|
+-----+----+

+-----+----+
|empid|name|
+-----+----+
|  101|John|
+-----+----+



#get records purchase more than 30 days
from pyspark.sql.functions import to_date, datediff, current_timestamp
data2=df.withColumn('last_purchase_date', to_date('last_purchase_date', 'yyyy-MM-dd'))    
# data3=data2.withColumn('datadiff', to_date(current_timestamp() - 'last_purchase_date').cast("int")).select(data3.col("datadiff")>30).display()

data3=data2.filter(datediff(current_timestamp(),data2.last_purchase_date)>400)
data3.display()



data = [("Alice", "HR"), ("Bob", "Finance"), ("Charlie", "HR"), ("David", "Engineering"), ("Eve", "Finance")]
columns = ["employee_name", "department"]

df = spark.createDataFrame(data, columns)
# df.display()

from pyspark.sql.functions import count, col, rank
from pyspark.sql.window import Window

df_count = (
    df
    .groupBy("department")
    .agg(count("employee_name").alias("employee_count"))
)

df_count.display()

window_spec = Window.orderBy(col("employee_count").desc())

df_top = (
    df_count
    .withColumn("rnk", rank().over(window_spec))
    .filter(col("rnk") == 1)
  
)

display(df_top)


