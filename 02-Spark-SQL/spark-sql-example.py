from pyspark.sql import SparkSession, Row # type: ignore

#mapper function
def mapDataToRow(line):
    fields = line.split(",")
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), 
                      age=int(fields[2]), numFriends=int(fields[3]))

#Create SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

lines = spark.sparkContext.textFile("file:///github/PySpark/02-Spark-SQL/data/fakefriends.csv")
people = lines.map(mapDataToRow)

#infer schema, and register DataFrame as table
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

#SQL run over DataFrames
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

#print out
for teen in teenagers.collect():
    print(teen)

#use function instead of SQL
schemaPeople.groupBy("age").count().orderBy("age").show()

#stop spark
spark.stop()