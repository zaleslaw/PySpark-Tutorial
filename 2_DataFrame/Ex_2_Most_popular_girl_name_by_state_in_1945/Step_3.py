from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Find max values for unknown name for each state
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Popular girls names") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    filteredStateNames = stateNames \
        .where("Year=1945 and Gender='F'") \
        .select("Name", "State", "Count")

    stateAndCount = filteredStateNames \
        .groupBy("State") \
        .agg(f.max("Count").alias("max"))

    stateAndCount.show()

    spark.stop()
