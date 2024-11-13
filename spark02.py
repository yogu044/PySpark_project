from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SimpleAggregation").getOrCreate()

data = [("Udhaya", "CSE", "Data Engineer", 10000),
        ("Siva", "IT", "Cloud Engineer", 7000),
        ("Giri", "CSBS", "Data Analyst", 9000),
        ("yogu", "AIDS", "AI Engineer", 10000)]
columns = ['Name', 'Department', 'Skill', 'Salary']

df = spark.createDataFrame(data, columns)


df.show()