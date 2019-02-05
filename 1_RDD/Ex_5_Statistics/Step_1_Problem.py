from __future__ import print_function
from pyspark.sql import SparkSession

# Find stddev, mean and find anomalies

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Statistics") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    anomalInts = sc.parallelize([1, 1, 2, 2, 3, 150, 1, 2, 3, 2, 2, 1, 1, 1, -100, 2, 2, 3, 4, 1, 2, 3, 4], 3)

    spark.stop()
