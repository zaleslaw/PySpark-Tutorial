from __future__ import print_function

import math

from pyspark.sql import SparkSession


def exp(x):
    return math.exp(x)


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

    # Answer: [2.718281828459045, 7.38905609893065, 20.085536923187668, 54.598150033144236, 148.4131591025766,
    # 403.4287934927351, 1096.6331584284585, 2980.9579870417283, 8103.083927575384, 22026.465794806718]
    print(exponents.collect())

    spark.stop()
