from pyspark.sql import SparkSession, Window
from pyspark.sql.window import Window
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

spark = SparkSession.builder \
    .appName("HighestPaidJobTitles") \
    .getOrCreate()

worker_data = [(1, 'John', 'Doe', 10000, '2023-01-01', 'Engineering'),
        (2, 'Jane', 'Smith', 12000, '2022-12-01', 'Marketing'),
        (3, 'Alice', 'Johnson', 12000, '2022-11-01', 'Engineering')]
columns = ['worker_id', 'first_name', 'last_name', 'salary', 'joining_date', 'department']
worker = spark.createDataFrame(worker_data, columns)

title_data = [(1, 'Engineer', '2022-01-01'),
        (2, 'Manager', '2022-01-01'),
        (3, 'Engineer', '2022-01-01')]
columns = ['worker_ref_id', 'worker_title', 'affected_from']
title = spark.createDataFrame(title_data, columns)
# title.show()

joindf=title.join(worker,worker.worker_id==title.worker_ref_id)
# joindf.show()

rankedf=joindf.withColumn("ranked",f.rank().over(Window.orderBy(joindf["salary"].desc())))
# rankedf.show()

highestpaid_df=rankedf.filter(rankedf["ranked"]==1)
# highestpaid_df.show()
from pyspark.sql.functions import max, min, when

result_df = joindf.groupBy("worker_id", "first_name", "last_name", "salary", "department") \
    .agg(
        max("salary").alias("max_salary"),
        min("salary").alias("min_salary")
    )

df1=result_df.withColumn("category",when(result_df["salary"]==result_df["max_salary"],"Highest Salary")
                     .when(result_df["salary"]==result_df["min_salary"],"lower Salary")
                     .otherwise(None))
df1.show()


joindf
+-------------+------------+-------------+---------+----------+---------+------+------------+-----------+
|worker_ref_id|worker_title|affected_from|worker_id|first_name|last_name|salary|joining_date| department|
+-------------+------------+-------------+---------+----------+---------+------+------------+-----------+
|            1|    Engineer|   2022-01-01|        1|      John|      Doe| 10000|  2023-01-01|Engineering|
|            3|    Engineer|   2022-01-01|        3|     Alice|  Johnson| 12000|  2022-11-01|Engineering|
|            2|     Manager|   2022-01-01|        2|      Jane|    Smith| 12000|  2022-12-01|  Marketing|
+-------------+------------+-------------+---------+----------+---------+------+------------+-----------+

resultdf
+---------+----------+---------+------+-----------+----------+----------+
|worker_id|first_name|last_name|salary| department|max_salary|min_salary|
+---------+----------+---------+------+-----------+----------+----------+
|        1|      John|      Doe| 10000|Engineering|     10000|     10000|
|        3|     Alice|  Johnson| 12000|Engineering|     12000|     12000|
|        2|      Jane|    Smith| 12000|  Marketing|     12000|     12000|
+---------+----------+---------+------+-----------+----------+----------+
df1
+---------+----------+---------+------+-----------+----------+----------+--------------+
|worker_id|first_name|last_name|salary| department|max_salary|min_salary|      category|
+---------+----------+---------+------+-----------+----------+----------+--------------+
|        1|      John|      Doe| 10000|Engineering|     10000|     10000|Highest Salary|
|        3|     Alice|  Johnson| 12000|Engineering|     12000|     12000|Highest Salary|
|        2|      Jane|    Smith| 12000|  Marketing|     12000|     12000|Highest Salary|
+---------+----------+---------+------+-----------+----------+----------+--------------+
