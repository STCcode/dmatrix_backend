from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("csvdemo-mysql").config("spark.some.config.option", "sale").getOrCreate() 
df = spark.read.csv("E:/working-directory/BITOOL_DONE/datasource/csvFiles/2019-08-24-15-20_dgr.csv",header=True,mode="DROPMALFORMED",sep=",").fillna("0") 
df.write.format("jdbc").options(url="jdbc:mysql://127.0.0.1:3306/db_demo",driver="com.mysql.cj.jdbc.Driver",dbtable="tbl_csvdemo",user="root",password="Admin@123").mode("overwrite").save() 
