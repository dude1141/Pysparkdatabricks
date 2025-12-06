from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.streaming import StreamingContext
import traceback

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)

# use a normal string for host
lines = ssc.socketTextStream("ec2-44--237-228.compute-1.amazonaws.com", 1234)
lines.pprint()

def getSparkSessionInstance(sparkConf):
    if "sparkSessionSingletonInstance" not in globals():
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # skip empty RDDs by checking partition count instead
        if rdd.getNumPartitions() == 0:
            print("empty rdd")
            return

        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda x: x.split(",")).map(lambda x: (x[0], x[1], x[2]))
        res = spark.createDataFrame(rowRdd, schema=["name", "age", "city"])
        res.show()
        hyd = res.where(col("city") == "hyd")
        # hyd.write.mode("append").format("jdbc").option(...).save()
    except Exception:
        traceback.print_exc()

lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
