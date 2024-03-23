
#import --you need to install jdbc driver in the snowflake cluster under libraries and restart , then it connects to mysql db


# Databricks notebook source
myconf = {
      "url":"jdbc:mysql://mysqzi.us-east-2.rds.amazonaws.com:3306/mysqldb",
      "user":"admin",
      "password":"Mypasswd",
      "driver":"com.mysql.cj.jdbc.Driver"
}

sfconf = {
      "sfURL":"vnsnowflakecomputing.com",
      "sfUser":"MITHLESHSNFLK",
      "sfPassword":"M41",
      "sfDatabase":"MITDB",
      "sfSchema":"PUBLIC",
      "sfWarehouse":"COMPUTE_WH"
}

#https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.12/2.13.0-spark_3.3
#https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc/3.14.4
#https://mvnrepository.com/artifact/net.snowflake/snowflake-ingest-sdk/2.0.2



# qry = """(select table_name from information_schema.TABLES WHERE TABLE_SCHEMA='mysqldb') tmp"""
tabs=["emp"]
for x in tabs:
      print('importing data from :',x)
      tab=str(x+"mar10")
      df=spark.read.format("jdbc").options(**myconf).option("dbtable",x).load()
      ndf=df.na.fill(0).distinct()
      ndf.write.mode("append").format("net.snowflake.spark.snowflake").options(**sfconf).option("dbtable",tab).save()

# this above query will read data from emp mysql and write to snwoflake table

# df=spark.read.format("jdbc").options(**myconf).option("dbtable","emp").load()
# df.show()

# COMMAND ----------


