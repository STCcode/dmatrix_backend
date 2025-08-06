from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("ssd").config("spark.some.config.option", "sale").getOrCreate() 
df = spark.read.csv("E:/working-directory/BITOOL_DONE/datasource/csvFiles/sales_QTY_LM-CM.csv",header=True,mode="DROPMALFORMED",sep=",").fillna("0") 
df.write.format("jdbc").options(url="jdbc:mysql://192.168.10.63:3306/ams_ph_2",driver="com.mysql.jdbc.Driver",dbtable="bb",user="root",password="Admin@123").mode("overwrite").save() 
