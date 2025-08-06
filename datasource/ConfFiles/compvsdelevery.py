from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField 
from pyspark.sql.types import DoubleType, IntegerType, StringType,FloatType 
schema = StructType([StructField("S_Plant", StringType()),StructField("S_Plant_name", StringType()),StructField("S_Region", StringType()),StructField("S_Material group", StringType()),StructField("S_Material_grp_name", StringType()),StructField("S_Calendar_month", StringType()),StructField("S_Fiscal_Year", StringType()),StructField("N_Total_no_notifs", FloatType()),StructField("N_Count", FloatType())]) 
spark = SparkSession.builder.appName("compvsdelevery").config("spark.some.config.option", "sale").getOrCreate() 
df = spark.read.csv("D:/Shikha_working_directory/bi-vertib/datasource/csvFiles/comvsdel.csv",header=True,mode="DROPMALFORMED",sep=",",schema=schema) 
df.write.format("jdbc").options(url="jdbc:mysql://192.168.10.63:3306/epinsight",driver="com.mysql.cj.jdbc.Driver",dbtable="tbl_complaint_delevery",user="root",password="Admin@123").mode("overwrite").save() 
