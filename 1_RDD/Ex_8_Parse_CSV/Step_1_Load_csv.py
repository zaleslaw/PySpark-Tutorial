from __future__ import print_function
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("ParseCSVWithRDD") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    # read from file
    stateNamesCSV = sc.textFile("/home/zaleslaw/data/StateNames.csv")
    # split / clean data
    headerAndRows = stateNamesCSV.map(lambda line: line.split(","))
    print(headerAndRows.take(10))
    # get header
    header = headerAndRows.first()
    # filter out header (eh. just check if the first val matches the first header name)
    data = headerAndRows.filter(lambda x: x[0] != header[0])
    print(data.take(5), sep='\n')
    # splits to map (header/value pairs)
    stateNames = data.map(lambda splits: dict(zip(header, splits)))
    # print top-5
    print(stateNames.take(5), sep='\n')

    spark.stop()
