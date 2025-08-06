from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("pgtopg").config("spark.some.config.option", "sale").getOrCreate() 
jdbcDF = spark.read.format("jdbc").options(url="jdbc:postgresql://localhost:5432/db_p9",driver="org.postgresql.Driver",dbtable="tbl_demo",user="postgres",password="Admin@123").load() 
jdbcDF.write.format("jdbc").options(url="jdbc:postgresql://localhost:5432/db_p9",driver="org.postgresql.Driver",dbtable="tbl_demo2",user="postgres",password="Admin@123").mode("overwrite").save() 
