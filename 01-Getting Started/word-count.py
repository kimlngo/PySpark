import re
from pyspark import SparkContext, SparkConf # type: ignore

def normalizeWord(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

def matchWordCount(word):
    return (word, 1)

def reduceBySum(v1, v2):
    return v1 + v2

def flipTuple(tup):
    return (tup[1], tup[0])

conf = SparkConf().setMaster("local").setAppName("WordCountApp")
sc = SparkContext(conf = conf)

wordCount = (sc.textFile("file:///github/PySpark/01-Getting Started/book.txt")
           .flatMap(normalizeWord)
           .map(matchWordCount)
           .reduceByKey(reduceBySum)
           .map(flipTuple)
           .sortByKey()
           .collect())

for count, word in wordCount:
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(f"{cleanWord} \t\t {count}")