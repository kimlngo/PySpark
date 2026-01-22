from pyspark import SparkContext, SparkConf # type: ignore

def parseLine(line):
    fields = line.split(",")
    stationId = fields[0]
    type = fields[2]
    temp = float(fields[3]) * 0.1
    return (stationId, type, temp) #(EZE00100082,TMAX,-10)

def reduceMaxTemp(t1, t2):
    return max(t1, t2)

conf = SparkConf().setMaster("local").setAppName("MinTempIn1800")
sc = SparkContext(conf = conf)

allMinTemps = (sc.textFile("file:///github/PySpark/01-Getting Started/1800.csv")
             .map(parseLine)
             .filter(lambda entry: "TMAX" in entry[1]) #(EZE00100082,TMAX,-10)
             .map(lambda entry: (entry[0], entry[2]))
             .reduceByKey(reduceMaxTemp)
             .collect())

for k, v in allMinTemps:
    print(f"{k}: {v:.2f}C")
