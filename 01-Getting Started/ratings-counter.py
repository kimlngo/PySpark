from pyspark import SparkConf, SparkContext # type: ignore
import collections

#execution command: spark-submit ratings-counter.py
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///github/PySpark/01-Getting Started/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(f"{key} {value}")
