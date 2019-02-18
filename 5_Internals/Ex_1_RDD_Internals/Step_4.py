import os

from pyspark.sql import SparkSession

# Union with another RDD
if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("RDD_Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    r = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    ints = sc.parallelize(r, 4)

    print(ints.toDebugString())
    print(ints.collect())

    # glom prints all partitions
    print(ints.glom().collect())

    # Step 1: Transform each number to its square
    squares = ints.map(lambda x: x * x)

    # Step 2: Filter even numbers
    even = squares.filter(lambda x: x % 2 == 0)

    # glom prints all partitions
    print(even.glom().collect())

    union = even.union(ints)
    # How many partitions will be here?
    # print(union.repartition(7).glom().collect)

    spark.stop()
