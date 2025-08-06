from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField 
from pyspark.sql.types import DoubleType, IntegerType, StringType,FloatType 
schema = StructType([StructField("s_Plant", StringType()),StructField("S_plant_name", StringType()),StructField("S_Region", StringType()),StructField("Material group", StringType()),StructField("Matgroup_name", StringType()),StructField("Calendar_month", StringType()),StructField("Fiscalyear", StringType()),StructField("Total no. notifs", StringType()),StructField("N_Count", FloatType()),StructField("Revenue", StringType())]) 
spark = SparkSession.builder.appName("GGF").config("spark.some.config.option", "sale").getOrCreate() 
df = spark.read.csv("D:/Shikha_working_directory/bi-vertib/datasource/csvFiles/newcomvsdelZQM_M07_Q001_EPINSIGHT.csv",header=True,mode="DROPMALFORMED",sep=",",schema=schema) 
df.write.format("jdbc").options(url="jdbc:mysql://192.168.10.63:3306/epinsight",driver="com.mysql.cj.jdbc.Driver",dbtable="TBL_COMPLAINT",user="root",password="Admin@123").mode("overwrite").save() 
