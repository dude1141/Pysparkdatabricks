# Pysparkdatabricks
tuning parametrs:

PERFORMANCE ISSUES:
1) Experiment with different partition sizes:
2) Spark.sql.shuffle.partitions and spark.default.parallelism.
3) use spark windowing or aggregate functions
4) This can be done by setting the log level to a higher severity level, such as WARN or ERROR

JOIN STRATEGIC:
1) BROADCAST JOIN (SMALL DATASET----JOIN WITH OTHER DATASETS).
2) BOTH DATASETS HUGE SORT -MERGE JOIN .



1) unitycatalogue: https://studio.youtube.com/video/sVc9m1sQxiY 
2) tables masking uc: https://youtu.be/Ye96j4YcAMU 
3) fullload and scd1 using databricks and azuresql: https://youtu.be/S2MF_g6viaA


basics:
Easy rule
For calculations, filters, conditions → often ** use F.col() **
For aggregate functions → usually use column name string directly
Example
** df.groupBy("category").agg(
    F.sum("quantity").alias("total_qty")
)  **

But:

** df.withColumn("total", F.col("price") * F.col("quantity")) **
