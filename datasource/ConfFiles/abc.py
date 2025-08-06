from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("abc").config("spark.some.config.option", "sale").getOrCreate() 
jdbcDF = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/db_pesio",driver="com.mysql.cj.jdbc.Driver",query="select `N_CATEGORY_ID` as `N_CATEGORY_ID`,`S_CATEGORY_NAME` as `S_CATEGORY_NAME`,`S_CREATED_BY` as `S_CREATED_BY`,`S_MODIFIED_BY` as `S_MODIFIED_BY`,`D_CREATED_DATE` as `D_CREATED_DATE`,`D_MODIFIED_DATE` as `D_MODIFIED_DATE` from tbl_category_master",user="root",password="Admin@123").load() 
jdbcDF.write.format("jdbc").options(url="jdbc:mysql://127.0.0.1:3306/db_demo",driver="com.mysql.cj.jdbc.Driver",dbtable="tbl_d3",user="root",password="Admin@123").mode("overwrite").save() 
