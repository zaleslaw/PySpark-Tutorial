from __future__ import print_function
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("RDD_Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # SAMPLE-1: Make dataset based on range and extract RDD from it

    ds = spark.range(10000000)
    print("Count %i" % ds.count())
    print("Count in rdd %i" % ds.rdd.count())

    # SAMPLE-2: Make RDD based on Array with reverse order
    sc = spark.sparkContext
    #r = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    #ints = sc.parallelize(r, 3)

    #ints.saveAsTextFile("/home/zaleslaw/data/ints")

    cachedInts = sc.textFile("/home/zaleslaw/data/ints").map(lambda x: int(x)).cache()

    squares = cachedInts.map(lambda x: x * x)

    print(squares.collect())

    spark.stop()
