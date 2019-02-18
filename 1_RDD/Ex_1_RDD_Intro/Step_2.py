from __future__ import print_function
from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("RDD_Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # SAMPLE-2: Make RDD based on Array with reverse order
    sc = spark.sparkContext
    r = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    ints = sc.parallelize(r, 3)
    print(ints.collect())

    ## Works for Ubuntu, in Windows do it manually
    #if os.path.exists("/home/zaleslaw/data/ints"):
    #    os.remove("/home/zaleslaw/data/ints")

    ints.saveAsTextFile("/home/zaleslaw/data/ints")

    spark.stop()
