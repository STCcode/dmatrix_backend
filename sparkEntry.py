
from pyspark import SparkConf, SparkContext, SQLContext

conf = SparkConf().setMaster("local[*]").setAppName("My App")
sc = SparkContext.getOrCreate(conf=conf)
#sc = SparkContext(conf=conf)
spark = SQLContext(sc)
