from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("mysql-mysql").config("spark.some.config.option", "sale").getOrCreate() 
jdbcDF = spark.read.format("jdbc").options(url="jdbc:mysql://192.168.10.63:3306/ams_ph_2",driver="com.mysql.cj.jdbc.Driver",dbtable="tbl_germany_sale",user="root",password="Admin@123").load() 
jdbcDF.write.format("jdbc").options(url="jdbc:mysql://127.0.0.1:3306/db_demo",driver="com.mysql.cj.jdbc.Driver",dbtable="tbl_sale",user="root",password="Admin@123").mode("overwrite").save() 
