from pyspark.sql import SparkSession  
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
schema = StructType([\
    StructField("date", IntegerType()),\
    StructField("Location", StringType()), \
    StructField("Idnum", IntegerType())\
    
])

scSpark = SparkSession \
    .builder \
    .appName("Python Spark SQL example: Reading CSV file with schema") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sdfData = scSpark.read.csv("E://working-directory//BITOOL_DONE//datasource//csvFiles//demospark.csv", header=True, sep=",", schema=schema)
sdfData.write.format("jdbc").options(url="jdbc:mysql://127.0.0.1:3306/db_demo",driver="com.mysql.jdbc.Driver",dbtable="tbl_spark",user="root",password="Admin@123").mode("overwrite").save()