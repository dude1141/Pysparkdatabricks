
val sparkconf= new sparkconf()

sparkconf.set("sparkappname","sparname")


'val spark=sparksession.builder
.config(sparkconf)
.getorcreate


UTF8 algo supports 200 languages

to handle corrupt data , readmoses comes in to picture
permissive---whihc row is corrupted it will be nullthat row.
dropmalformed--corrupted row will be deleted.
failfast.



with option("inferschema",true) above read modes will not work


if you dont define explicit schema these dont work
spark.read.format().option(header).option(path).load()


id name age
1 xx 35
2 yy 57


if you dont want to filter the data while reading then its ddl approach as below
val ddlschema="id intm name String, City, String"

val spark.read(Csv).option(header).schema(ddlschema).option("mode","FAIFAST").option("path","C://path").load()
now option("dropmalformed"),true will work or modes will work.
if there aremillion records , FAILFASTSAFE will throw error. if there \
are corrupted records.





if you want to filter some data whileloading then go with programmatic
val pgschema= StructType (List(
StructField("id", IntegerType, nullable=false)
StructField("name", StringType, nullable=true)
StructField ("age",IntegerType,nullable=true)
))




