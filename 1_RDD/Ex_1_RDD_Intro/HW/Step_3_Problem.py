from __future__ import print_function
from pyspark.sql import SparkSession

# Implement function that returns exponent
def exp(x):
    pass


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("RDD_Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # SAMPLE-3: Load from file and calculate squares
    sc = spark.sparkContext

    cachedInts = sc.textFile("/home/zaleslaw/data/ints").map(lambda x: int(x)).cache()

    exponents = cachedInts.map(lambda x: exp(x))

    print(exponents.collect())

    spark.stop()
