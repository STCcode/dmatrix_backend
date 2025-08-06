from pyspark.sql import SparkSession  
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
schema = StructType([\
    StructField("date2", IntegerType()),\
    StructField("Location2", StringType()), \
    StructField("Idnum2", IntegerType())\
    
])

scSpark = SparkSession \
    .builder \
    .appName("Python Spark SQL example: Reading CSV file with schema") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


jdbcDF = scSpark.read.format("jdbc").options(url="jdbc:mysql://127.0.0.1:3306/db_demo",driver="com.mysql.cj.jdbc.Driver",query="select date as date2,Location as Location3 ,Idnum as idnum3 from tbl_spark",user="root",password="Admin@123").load() 
jdbcDF.write.format("jdbc").options(url="jdbc:mysql://127.0.0.1:3306/db_demo",driver="com.mysql.jdbc.Driver",dbtable="tbl_spark3",user="root",password="Admin@123").mode("overwrite").save()