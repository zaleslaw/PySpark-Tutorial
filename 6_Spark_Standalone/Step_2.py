from __future__ import print_function
from pyspark.sql import SparkSession

# Make the same operations like in the previous RDD CSV example
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("spark://172.24.0.4:7077") \
        .appName("DataFrame Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    nationalNames = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs://172.24.0.3:8020/tmp/NationalNames.csv")

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

    nationalNames.write.mode("overwrite").json("hdfs://172.24.0.3:8020/tmp/nationalNames")

    spark.stop()
