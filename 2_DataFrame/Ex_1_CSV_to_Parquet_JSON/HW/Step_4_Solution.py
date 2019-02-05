from __future__ import print_function
from pyspark.sql import SparkSession

# Print out the registered births by year in US since 1880
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

    nationalNames.cache()

    print("Registered births by year in US since 1880")
    nationalNames \
        .groupBy("Year") \
        .sum("Count") \
        .orderBy("Year") \
        .show(200)

    spark.stop()
