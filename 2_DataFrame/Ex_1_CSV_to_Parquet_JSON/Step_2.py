from __future__ import print_function
from pyspark.sql import SparkSession

# Write to parquet file with overwrite mode
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

    stateNames.write.mode("overwrite").parquet("/home/zaleslaw/data/stateNames")

    spark.stop()
