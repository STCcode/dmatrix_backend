from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("catdata").config("spark.some.config.option", "sale").getOrCreate() 
jdbcDF = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/db_pesio",driver="com.mysql.cj.jdbc.Driver",dbtable="tbl_category_master",user="root",password="Admin@123").load() 
jdbcDF.write.format("jdbc").options(url="jdbc:postgresql://localhost:5432/db_p9",driver="org.postgresql.Driver",dbtable="tbl_cat_master",user="postgres",password="Admin@123").mode("overwrite").save() 
