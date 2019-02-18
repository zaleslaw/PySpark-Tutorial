from __future__ import print_function

import os

from pyspark.sql import SparkSession

# Print first element in RDD
# Print first two elements
# Print first two elements in ordered RDD (reverse order)

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

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

    # Step 2: Filter even numbers
    even = squares.filter(lambda x: x % 2 == 0)
    print(even.collect())

    # Step 3: print RDD metadata
    even.setName("Even numbers")
    print("Name is " + str(even.name()) + " id is " + str(even.id()))
    print(even.toDebugString())

    # Step 4: print multiplication of all items
    print("Total multiplication is " + str(even.reduce(lambda a, b: a * b)))

    print(even.first())
    print(even.take(2))
    print(even.takeOrdered(2, key=lambda x: -x))

    spark.stop()
