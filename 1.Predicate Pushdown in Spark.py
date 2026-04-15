# Databricks notebook source
# MAGIC %md
# MAGIC ###Predicate Pushdown
# MAGIC
# MAGIC *Predicate Pushdown is an optimization technique that allows filter conditions (predicates) to be applied directly at the data source level. Instead of loading all the data into Spark and then filtering it, the filtering is done as close to the data source as possible. This approach minimizes the amount of data that needs to be read and processed by Spark, leading to enhanced performance.*
# MAGIC
# MAGIC ![](/files/images/Predicate_Pushdown_first_pic1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ***Input Data Setup***

# COMMAND ----------

sales_data = [
    ("2024-11-30", 101, 5, 500),
    ("2024-12-01", 101, 3, 300),
    ("2024-12-15", 102, 2, 200),
    ("2024-12-20", 101, 7, 700),
    ("2024-12-25", 103, 8, 800),
    ("2025-01-01", 104, 4, 400),
    ("2025-01-02", 103, 6, 600)
]

schema = ["sale_date", "product_id", "quantity", "amount"]
sales_df = spark.createDataFrame(sales_data, schema)

dataset_path = "/dbfs/FileStore/input_files/08week/sales_predicate_data"
sales_df.write.mode("overwrite").format("parquet").save(dataset_path)

# COMMAND ----------

#Sample query to test the predicate pushdown

full_sales_df = spark.read.parquet("/dbfs/FileStore/input_files/08week/sales_predicate_data")
display(full_sales_df)
filtered_sales_df = full_sales_df.filter("product_id = 101")
display(filtered_sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/files/images/PredicatePushdown.png)

# COMMAND ----------

filtered_sales_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC | Feature/Aspect           | With Predicate Pushdown                       | Without Predicate Pushdown                    |
# MAGIC |--------------------------|------------------------------------------------|-----------------------------------------------|
# MAGIC | ***Data Filtering***       | Filters are applied at the data source level.  | Filters are applied after data is loaded.     |
# MAGIC | ***Data Volume Read***     | Only the relevant subset of data is read.      | The entire dataset is read into memory.       |
# MAGIC | ***I/O Operations***       | Reduced I/O since fewer data blocks are read.  | Higher I/O as all data blocks are accessed.   |
# MAGIC | ***Network Traffic***      | Minimal network traffic due to reduced data transfer. | Increased network traffic as all data is transferred. |
# MAGIC | ***Memory Usage***         | Lower memory usage since less data is loaded.  | Higher memory usage due to loading all data.  |
# MAGIC | ***Performance***          | Typically faster query execution.              | Slower query execution due to larger data load. |
# MAGIC | ***Execution Plan***       | Filter operations are visible as "PushedFilters" in the plan. | Filter operations occur after data scan.     |
# MAGIC | ***Applicable Scenarios*** | Most beneficial for large datasets with selective filters. | Useful for scenarios requiring full dataset analysis. |
# MAGIC | ***Compatibility***        | Supported by columnar formats like Parquet, ORC. | Works with all data formats, including text files. |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### FYI:
# MAGIC - ***Before Spark 1.2:*** *There was no predicate pushdown support.*
# MAGIC - ***Spark 1.2 Onwards:*** *Predicate pushdown was introduced and gradually improved.*
# MAGIC - ***Spark 3.0+:*** *Brought substantial improvements in predicate pushdown and other optimizations.*

# COMMAND ----------


#disable Spark's entire SQL optimizer (Catalyst) 
spark.conf.set("spark.sql.optimizer.wholeStageCodegen.enabled", "false")
spark.conf.get("spark.sql.optimizer.wholeStageCodegen.enabled")

# COMMAND ----------


#disable spark AQE
spark.conf.set("spark.sql.adaptive.enabled","false")
spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------


#Disable predicate pushdown
spark.conf.set("spark.sql.parquet.filterPushdown", "false")
spark.conf.get("spark.sql.parquet.filterPushdown")

# COMMAND ----------

# MAGIC %md
# MAGIC ***Note:*** *Instead of disabling the above settings, to test the scenario effectively, it's better to execute the queries directly in their respective Spark versions and observe the differences in behavior.*