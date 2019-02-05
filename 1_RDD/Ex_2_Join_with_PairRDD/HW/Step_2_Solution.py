from __future__ import print_function

import os

from pyspark.sql import SparkSession

# Get amount of values by each key in joinResult
# Get values by key from PairRDD
# Get keys and values separately

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Join_with_PairRDD") \
        .getOrCreate()

    os.environ["PYTHONHASHSEED"] = "123"

    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    # SAMPLE-1: Make dataset based on range and extract RDD from it

    # A few developers decided to commit something
    # Define pairs <Developer name, amount of commited core lines>

    codeRows = sc.parallelize([("Ivan", 240), ("Elena", -15), ("Petr", 39), ("Elena", 290)])

    programmerProfiles = sc.parallelize([("Ivan", "Java"), ("Elena", "Scala"), ("Petr", "Scala")])

    joinResult = programmerProfiles.join(codeRows)

    print(joinResult.countByKey())
    print(joinResult.lookup("Elena"))
    print(joinResult.values().collect())
    print(joinResult.keys().collect())

    spark.stop()
