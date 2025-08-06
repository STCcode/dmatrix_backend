from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("sparkdatasource").config("spark.some.config.option", "sale").getOrCreate() 
jdbcDF = spark.read.format("jdbc").options(url="jdbc:mysql://192.168.10.63:3306/epinsight",driver="com.mysql.cj.jdbc.Driver",dbtable="tbl_cap_base_rate",user="root",password="Admin@123").load() 
jdbcDF.write.format("jdbc").options(url="jdbc:mysql://192.168.10.63:3306/ams_ph_2",driver="com.mysql.cj.jdbc.Driver",dbtable="tbl_tpdaTA",user="root",password="Admin@123").mode("overwrite").save() 
