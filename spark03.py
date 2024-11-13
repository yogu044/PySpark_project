
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print(spark)


spark.stop()
