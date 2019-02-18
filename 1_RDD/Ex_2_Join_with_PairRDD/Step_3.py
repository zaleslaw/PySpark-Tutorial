from __future__ import print_function

import os

from pyspark.sql import SparkSession

# Use cogroup operation
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

    programmerProfiles = sc.parallelize([("Ivan", "Java"), ("Elena", "Scala"), ("Petr", "Scala")])

    print(programmerProfiles.cogroup(codeRows).collect())

    spark.stop()
