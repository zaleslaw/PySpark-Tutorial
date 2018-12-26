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
    stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    stateNames.show()
    stateNames.createOrReplaceTempView("stateNames")

    spark.sql("SELECT DISTINCT Name FROM stateNames WHERE Gender = 'M' ORDER BY Name").show(100)

    nationalNames = spark.read.json("/home/zaleslaw/data/nationalNames")

    nationalNames.createOrReplaceTempView("nationalNames")

    result = spark.sql(
        "SELECT nyYear as year, stateBirths/usBirths as proportion, stateBirths, usBirths FROM (SELECT year as nyYear, SUM(count) as stateBirths FROM stateNames WHERE state = 'NY' GROUP BY year ORDER BY year) as NY" +
        " JOIN (SELECT year as usYear, SUM(count) as usBirths FROM nationalNames GROUP BY year ORDER BY year) as US ON nyYear = usYear")

    result.show(150)
    result.explain(True)

    spark.stop()
