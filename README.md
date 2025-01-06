# Pysparkdatabricks
tuning parametrs:

Performance issues:
Experiment with different partition sizes:
Spark.sql.shuffle.partitions and spark.default.parallelism.
use spark windowing or aggregate functions

JOIN STRATEGIC- 
    1) BROADCAST JOIN (SMALL DATASET----JOIN WITH OTHER DATASETS).
    2) BOTH DATASETS HUGE SORT -MERGE JOIN .
