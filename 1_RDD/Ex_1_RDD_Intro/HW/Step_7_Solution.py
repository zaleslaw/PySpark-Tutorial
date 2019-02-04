from __future__ import print_function
from pyspark.sql import SparkSession

# Print groups and aggregation operation correctly
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

    # Step 2: Filter even numbers
    even = squares.filter(lambda x: x % 2 == 0)
    print(even.collect())

    # Step 3: print RDD metadata
    even.setName("Even numbers")
    print("Name is " + str(even.name()) + " id is " + str(even.id()))
    print(even.toDebugString())

    # Step 4: print multiplication of all items
    print("Total multiplication is " + str(even.reduce(lambda a, b: a * b)))

    # Step 5: Transform to PairRDD make keys 0 for even and 1 for odd numbers and
    groups = cachedInts.map(lambda x: (0, x) if (x % 2 == 0) else (1, x))

    print("-- Groups plan --")
    print(groups.groupByKey().toDebugString(), sep='\n')
    print("-- Group by key --")
    print(groups.groupByKey().mapValues(list).collect())

    print("-- Count group elements by key --")
    print(groups.countByKey())

    spark.stop()
