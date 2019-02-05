from __future__ import print_function
from pyspark.sql import SparkSession

# To scan parquet is more effective than scan of CSV files
# Get most popular girl name in each state in 1945 or in another year
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Popular girls names") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    stateNames.show()
    stateNames.printSchema()

    spark.stop()
