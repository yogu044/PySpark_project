from pyspark.sql import SparkSession
from pyspark.sql.functions import desc


spark = SparkSession.builder \
    .appName("WordCountApp") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

data = ["hello world", "hello Spark", "hello hello world"]

rdd = spark.sparkContext.parallelize(data)
result_rdd = rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

print("RDD-based word count results:", result_rdd.take(10))

df = spark.createDataFrame([(line,) for line in data], ["value"])
result_df = df.selectExpr("explode(split(value, ' ')) as word") \
    .groupBy("word").count().orderBy(desc("count"))

print("DataFrame-based word count results:")
result_df.show(10)

# Stop the Spark session
spark.stop()
