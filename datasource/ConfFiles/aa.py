from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("aa").config("spark.some.config.option", "sale").getOrCreate() 
jdbcDF = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/db_pesio",driver="com.mysql.cj.jdbc.Driver",query="select `N_CATEGORY_ID` as `CATEGORY_ID`,`S_CATEGORY_NAME` as `CATEGORY_NAME`,`S_CREATED_BY` as `S_CREATED_BY`,`S_MODIFIED_BY` as `S_MODIFIED_BY`,`D_CREATED_DATE` as `D_CREATED_DATE`,`D_MODIFIED_DATE` as `D_MODIFIED_DATE` from tbl_category_master",user="root",password="Admin@123").load() 
jdbcDF.write.format("jdbc").options(url="jdbc:postgresql://localhost:5432/db_p9",driver="org.postgresql.Driver",dbtable="tbl_cat",user="postgres",password="Admin@123").mode("overwrite").save() 
