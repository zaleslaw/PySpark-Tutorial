from __future__ import print_function
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("DataFrame Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Id as int, count as int due to one extra pass over the data
    stateNames = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/home/zaleslaw/data/StateNames.csv")

    stateNames.show()
    stateNames.printSchema()

    # stateNames.write.parquet("/home/zaleslaw/data/stateNames")

    stateNames.cache()

    nationalNames = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/home/zaleslaw/data/NationalNames.csv")

    nationalNames.show()
    nationalNames.printSchema()
    nationalNames.write.json("/home/zaleslaw/data/nationalNames")

    # Step - 3: Simple dataframe operations

    # filter & select & orderBy
    nationalNames \
        .where("Gender == 'M'") \
        .select("Name", "Year", "Count") \
        .orderBy("Name", "Year") \
        .show(100)

    # Registered births by year in US since 1880
    nationalNames \
        .groupBy("Year") \
        .sum("Count") \
        .orderBy("Year") \
        .show(200)

    spark.stop()
