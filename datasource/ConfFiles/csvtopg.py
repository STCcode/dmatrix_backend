from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("csvtopg").config("spark.some.config.option", "sale").getOrCreate() 
df = spark.read.csv("E:/working-directory/BITOOL_DONE/datasource/csvFiles/2019-08-24-15-20_dgr.csv",header=True,mode="DROPMALFORMED",sep=",").fillna("0") 
df.write.format("jdbc").options(url="jdbc:postgresql://localhost:5432/db_p9",driver="org.postgresql.Driver",dbtable="tbl_demo",user="postgres",password="Admin@123").mode("overwrite").save() 
