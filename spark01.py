from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("SimpleAggregation") \
    .getOrCreate()

print("Spark Version:", spark.version)