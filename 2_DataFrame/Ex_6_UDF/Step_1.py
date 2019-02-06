from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf


# Define UDF and use in DF API

def isWorldWarTwoYearFunction(year):
    return True if (1939 <= year <= 1945) else False


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("DataFrame Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    stateNames.cache()

    # Step-1: add and register UDF function
    from pyspark.sql.types import BooleanType

    isWorldWarTwoYear = udf(lambda year: isWorldWarTwoYearFunction(year), BooleanType())

    # Step-2: use UDF in dataframe
    stateNames.select("Year", isWorldWarTwoYear(stateNames["Year"])).distinct().orderBy(stateNames["Year"].desc()).show(
        100)

    spark.stop()
