from __future__ import print_function
from pyspark.sql import SparkSession

# SAMPLE-3: Load from file and calculate squares
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("RDD_Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    sc = spark.sparkContext

    cachedInts = sc.textFile("/home/zaleslaw/data/ints").map(lambda x: int(x)).cache()

    # Step 1: Transform each number to its square
    squares = cachedInts.map(lambda x: x * x)

    print(squares.collect())

    spark.stop()
