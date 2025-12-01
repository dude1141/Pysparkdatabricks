# python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, get_json_object
import re
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"C:\bigdata\drivers\world_bank.json"

# Read JSON file
df = spark.read.json(data)
df.printSchema()
# Extract nested fields and arrays
result = df.select(
    col("id"),
    col("project_name"),
    col("countryname"),
    col("countrycode"),
    col("boardapprovaldate"),
    col("closingdate"),
    col("impagency"),
    col("lendinginstr"),
    col("lendprojectcost"),
    col("idacommamt"),
    col("project_abstract.cdata").alias("project_abstract"),
    col("sector1.Name").alias("sector1"),
    col("sector2.Name").alias("sector2"),
    col("sector3.Name").alias("sector3"),
    col("sector4.Name").alias("sector4"),
    col("theme1.Name").alias("theme1"),
    col("totalamt"),
    col("projectstatusdisplay"),
    explode("majorsector_percent").alias("majorsector_percent"),
    explode("projectdocs").alias("projectdoc")
)

# Further extract values from exploded structs
final = result.select(
    "id", "project_name", "countryname", "countrycode", "boardapprovaldate", "closingdate",
    "impagency", "lendinginstr", "lendprojectcost", "idacommamt", "project_abstract",
    "sector1", "sector2", "sector3", "sector4", "theme1", "totalamt", "projectstatusdisplay",
    col("majorsector_percent.Name").alias("majorsector_name"),
    col("majorsector_percent.Percent").alias("majorsector_percent"),
    col("projectdoc.DocTypeDesc").alias("doc_type_desc"),
    col("projectdoc.DocType").alias("doc_type"),
    col("projectdoc.DocURL").alias("doc_url"),
    col("projectdoc.DocDate").alias("doc_date")
)

#final.show(truncate=False)
#final.printSchema()
import re


def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    cols = [re.sub('[^a-zA-Z0-9]', "", c.lower()) for c in df.columns]
    df = df.toDF(*cols)
    return df


def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df)
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True
    return df;

ndf=flatten(df)
ndf.printSchema()