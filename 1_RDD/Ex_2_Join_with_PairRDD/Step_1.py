from __future__ import print_function

import os

from pyspark.sql import SparkSession

# Define programmer's data and reduce some data
if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("RDD_Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    # SAMPLE-1: Make dataset based on range and extract RDD from it

    # A few developers decided to commit something
    # Define pairs <Developer name, amount of commited core lines>

    codeRows = sc.parallelize([("Ivan", 240), ("Elena", -15), ("Petr", 39), ("Elena", 290)])

    # Let's calculate sum of code lines by developer
    print(codeRows.reduceByKey(lambda x, y: x + y).collect())

    # Or group items to do something else
    print(codeRows.groupByKey().mapValues(list).collect())

    spark.stop()
