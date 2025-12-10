from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import substring, lit, regexp_replace

# ---------------------------
# Spark Session
# ---------------------------
spark = SparkSession.builder \
    .appName("test") \
    .master("local[*]") \
    .getOrCreate()

# ---------------------------
# Load CSV — make sure path exists
# ---------------------------
data_path = r"C:\bigdata\drivers\aadharpancarddata.csv"

df = spark.read.csv(data_path, header=True, inferSchema=True)

print("Input Data:")
df.show(truncate=False)

# ---------------------------
# Masking Logic
# ---------------------------
df2 = (
    df
    # Clean Aadhaar (keep only digits)
    .withColumn(
        "aadhar_clean",
        F.regexp_replace(F.col("aadharcardnumber"), r"[^0-9]", "")
    )

    # Mask Aadhaar: **** + last 4 digits (Spark 3.2-safe)
    .withColumn(
        "aadhar_masked",
        F.expr("""
            concat(
                repeat('*', greatest(length(aadhar_clean) - 4, 0)),
                substring(aadhar_clean, greatest(length(aadhar_clean) - 3, 1), 4)
            )
        """)
    )

    # Email local
    .withColumn(
        "email_local",
        F.when(
            F.col("email").isNotNull() & F.col("email").contains("@"),
            F.split(F.col("email"), "@").getItem(0)
        )
    )

    # Email domain
    .withColumn(
        "email_domain",
        F.when(
            F.col("email").isNotNull() & F.col("email").contains("@"),
            F.split(F.col("email"), "@").getItem(1)
        )
    )

    .withColumn("wheneg", F.when(F.col("Email").contains("hotmail"), lit("removed")).otherwise(F.col("Email")))
    .withColumn("grade", F.when(F.col("Age") < 25, lit("minor"))
                         .when((F.col("Age") >= 25) & (F.col("Age") < 40), lit("adult"))
                         .otherwise(lit("senior citizen")))
    .withColumn("Emails", regexp_replace(F.col("Email"), "[@.() ]", ""))

    .withColumn("splits", F.split(F.col("Email"), "@")[0])
    .withColumn("email_local_new", F.col("splits").getItem(0))


    # Mask Email: keep first 2 chars of local part
    .withColumn(
        "email_masked",
        F.when(
            F.col("email_local").isNotNull() & (F.length("email_local") > 2),
            F.concat(
                F.substring(F.col("email_local"), 1, 2),
                F.expr("repeat('*', length(email_local) - 2)"),
                F.lit("@"),
                F.col("email_domain")
            )
        ).otherwise(F.col("email"))
    )

    .drop("aadhar_clean", "email_local", "email_domain")
)

df2.show(truncate=False)
