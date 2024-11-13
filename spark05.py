


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD-Demo").getOrCreate()


numbers = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(numbers)

rdd.collect()

data = [("sanjay", 25), ("mohan", 30), ("selva", 35)]
rdd = spark.sparkContext.parallelize(data)

print("All elements of rdd: ", rdd.collect())

count = rdd.count()
print("Total number of elements in rdd: ", count)

first_element = rdd.first()
print("The first element of the rdd: ", first_element)

rdd.foreach(lambda x: print(x))
mapped_rdd = rdd.map(lambda x: (x[0].upper(), x[1]))

result = mapped_rdd.collect()
print("rdd with uppercease name: ", result)

filtered_rdd = rdd.filter(lambda x: x[1] > 30)
filtered_rdd.collect()

reduced_rdd = rdd.reduceByKey(lambda x, y: x + y)
reduced_rdd.collect()

sorted_rdd = rdd.sortBy(lambda x: x[1], ascending=False)
sorted_rdd.collect()

spark.stop()