from __future__ import print_function
from pyspark.sql import SparkSession

# Read the second file and write to json
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("DataFrame Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    nationalNames = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/home/zaleslaw/data/NationalNames.csv")

    nationalNames.show()
    nationalNames.printSchema()
    nationalNames.write.mode("overwrite").json("/home/zaleslaw/data/nationalNames")

    spark.stop()
