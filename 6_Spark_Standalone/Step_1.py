from __future__ import print_function

import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

    spark = SparkSession \
        .builder \
        .master("spark://172.24.0.4:7077") \
        .appName("RDD_Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # SAMPLE-1: Make dataset based on range and extract RDD from it

    ds = spark.range(100000)
    print("Count %i" % ds.count())
    print("Count in rdd %i" % ds.rdd.count())

    ds.rdd.saveAsTextFile("hdfs://172.24.0.2:8020/tmp/numbers-as-text4") # doesn't work for python 3.4

    spark.stop()
