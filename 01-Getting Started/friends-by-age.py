from pyspark import SparkConf, SparkContext # type: ignore

#input:  0,Will,33,385
#output: k-v: (33, 385)
def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    friends = int(fields[3])
    return (age, friends)


conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///github/PySpark/01-Getting Started/fakefriends.csv")
rdd = lines.map(parseLine)
totalByAge = (rdd.mapValues(lambda val : (val, 1)) #(33, (385, 1))
                 .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))) #(33, (387, 2))

averageByAge = totalByAge.mapValues(lambda v : int(v[0] / v[1]))
results = averageByAge.sortByKey().collect()
print(type(results))

for r in results:
    print(r)
                