from pyspark.sql import *
from pyspark.sql.functions import *

#spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
#data=r"C:\bigdata\drivers\aadharpancarddata.csv"
#df=spark.read.csv(data,header=True,inferSchema=True)
#df.show()


from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("medallion").master("local[*]").getOrCreate()
data = r"C:\bigdata\drivers\aadharpancarddata.csv"

# Bronze Layer: Raw Data
bronze_df = spark.read.csv(data, header=True, inferSchema=True)
bronze_df.createOrReplaceTempView("bronze")

# Silver Layer: Cleaned/Validated Data using regex
# Example regex for Aadhar: 12 digits, PAN: 5 letters, 4 digits, 1 letter
silver_df = bronze_df \
    .withColumn("aadhar_valid", regexp_extract(col("aadhar"), r"^\d{12}$", 0) != "") \
    .withColumn("pan_valid", regexp_extract(col("pan"), r"^[A-Z]{5}\d{4}[A-Z]$", 0) != "")
silver_df.createOrReplaceTempView("silver")

# Gold Layer: Aggregated/Business Data
gold_df = silver_df.groupBy("aadhar_valid", "pan_valid").count()
gold_df.createOrReplaceTempView("gold")

# Show results for each layer
print("Bronze Layer:")
bronze_df.show()
print("Silver Layer:")
silver_df.show()
print("Gold Layer:")
gold_df.show()