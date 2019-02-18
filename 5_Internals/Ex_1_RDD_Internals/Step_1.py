import os

from pyspark.sql import SparkSession

# RDD with 4 partitions
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

    spark.stop()
