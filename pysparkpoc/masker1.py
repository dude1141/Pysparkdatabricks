from pyspark.sql import *
from pyspark.sql.functions import *

# spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
from pyspark.sql import SparkSession
from Datamasker import DataMasker

spark = SparkSession.builder.appName("MaskingApp").master("local[*]").getOrCreate()

data_path = r"C:\bigdata\drivers\aadharpancarddata.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

masker = DataMasker(spark)

df_masked = masker.mask_all(
    df,
    aadhaar_col="aadharcardnumber",
    email_col="email"
)

df_masked.show(truncate=False)
