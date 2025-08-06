from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("test").config("spark.some.config.option", "sale").getOrCreate() 
