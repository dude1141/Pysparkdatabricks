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
