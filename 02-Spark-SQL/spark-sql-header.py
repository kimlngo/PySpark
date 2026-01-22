from pyspark.sql import SparkSession # type: ignore

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///github/PySpark/02-Spark-SQL/data/fakefriends-header.csv")

print("Here is the inferred schema:")
people.printSchema()

print("Display the name column:")
people.select("name").show()

print("Show people under 21:")
people.filter(people.age < 21).show()

print("Group by age:")
people.groupBy("age").count().show()

print("Transform data of a column and create a new virtual column:")
people.select(people.name, people.age + 10).show()
spark.stop()