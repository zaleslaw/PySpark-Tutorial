from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Show Top girl name for each state for fixed year sorted by state and count
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

    filteredStateNames.orderBy(filteredStateNames['State'].desc(), filteredStateNames['Count'].desc()).show()

    spark.stop()
