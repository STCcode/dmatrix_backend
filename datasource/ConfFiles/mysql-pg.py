from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("mysql-pg").config("spark.some.config.option", "sale").getOrCreate() 
jdbcDF = spark.read.format("jdbc").options(url="jdbc:mysql://127.0.0.1:3306/db_pesio_new",driver="com.mysql.cj.jdbc.Driver",dbtable="tbl_customer_master",user="root",password="Admin@123").load() 
jdbcDF.write.format("jdbc").options(url="jdbc:postgresql://localhost:5432/db_p9",driver="org.postgresql.Driver",dbtable="tbl_cust_master",user="postgres",password="Admin@123").mode("overwrite").save() 
