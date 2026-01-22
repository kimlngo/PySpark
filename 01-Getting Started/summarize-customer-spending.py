from pyspark import SparkConf, SparkContext # type: ignore

conf = SparkConf().setMaster("local").setAppName("SummarizeCustomerSpending")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    custId = int(fields[0])
    amount = float(fields[2])
    return (custId, amount)


customerSpending = (sc.textFile("file:///github/PySpark/01-Getting Started/customer-orders.csv")
                        .map(parseLine)
                        .reduceByKey(lambda x, y: x + y)
                        .sortBy(lambda x: x[1]) #kind of sort by value
                        .collect())

for custId, amount in customerSpending:
    print(f'{custId}: {amount:.2f}')