from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col, date_add
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, DateType
from datetime import datetime

spark = SparkSession.builder.appName("CustomerTransactions").getOrCreate()

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("amount", DoubleType(), True)
])


data = [
    (1, datetime.strptime("2024-11-01", "%Y-%m-%d").date(), 100.0),
    (1, datetime.strptime("2024-11-02", "%Y-%m-%d").date(), 150.0),
    (1, datetime.strptime("2024-11-03", "%Y-%m-%d").date(), 200.0),
    (2, datetime.strptime("2024-11-01", "%Y-%m-%d").date(), 80.0),
    (2, datetime.strptime("2024-11-02", "%Y-%m-%d").date(), 120.0),
    (2, datetime.strptime("2024-11-03", "%Y-%m-%d").date(), 160.0)
]

transactions = spark.createDataFrame(data, schema=schema)

window_spec_cumulative = Window.partitionBy("customer_id").orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

window_spec_rolling_avg = Window.partitionBy("customer_id").orderBy("transaction_date").rowsBetween(-6, 0)  # Last 7 transactions

transactions_with_metrics = transactions.withColumn("cumulative_amount", sum("amount").over(window_spec_cumulative)) \
    .withColumn("rolling_avg_amount", avg("amount").over(window_spec_rolling_avg))

transactions_with_metrics.show()