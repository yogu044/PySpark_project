from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("SportsEcommerce").getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", DateType(), True)
])

data = [
    ("t1", "u1", "p1", "football", 100.0, datetime.strptime("2023-01-01", "%Y-%m-%d").date()),
    ("t2", "u1", "p2", "basketball", 50.0, datetime.strptime("2023-02-01", "%Y-%m-%d").date()),
    ("t3", "u1", "p3", "soccer", 150.0, datetime.strptime("2023-03-01", "%Y-%m-%d").date()),
    ("t4", "u2", "p4", "basketball", 200.0, datetime.strptime("2022-11-01", "%Y-%m-%d").date()),
    ("t5", "u2", "p5", "football", 80.0, datetime.strptime("2023-06-01", "%Y-%m-%d").date()),
    ("t6", "u2", "p6", "soccer", 100.0, datetime.strptime("2024-01-15", "%Y-%m-%d").date())
]

df = spark.createDataFrame(data, schema=schema)

user_spending = df.groupBy("user_id").agg(
    sum("amount").alias("total_spent"),
    avg("amount").alias("avg_transaction")
)

category_count = df.groupBy("user_id", "category").agg(count("*").alias("category_count"))

window_spec = Window.partitionBy("user_id").orderBy(col("category_count").desc())

favorite_category = category_count.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).select("user_id", "category")

result = user_spending.join(favorite_category, on="user_id").withColumnRenamed("category", "favorite_category")

print("Final Result with Total, Average, and Favorite Category:")
result.show()
