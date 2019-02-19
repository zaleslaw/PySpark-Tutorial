from __future__ import print_function
from pyspark.sql import SparkSession

# Make the same operations like in the previous RDD CSV example
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("spark://172.18.0.2:7077") \
        .appName("DataFrame Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    nationalNames = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/tmp/data/datasets/NationalNames.csv")

    print(nationalNames.is_cached)
    nationalNames.cache()

    nationalNames.show()
    print(nationalNames.count())
    print(nationalNames.is_cached)

    # filter & select & orderBy
    nationalNames \
        .where("Gender == 'M'") \
        .select("Name", "Year", "Count") \
        .orderBy("Name", "Year") \
        .show(100)

    spark.stop()
