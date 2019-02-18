import os

from pyspark.sql import SparkSession

# Persist with Spark SQL
# See the difference in plans with caching and without:
# InMemoryTableScan [Gender#3, Name#1], [isnotnull(Gender#3), (Gender#3 = M)]
#                       +- InMemoryRelation [Id#0, Name#1, Year#2, Gender#3, State#4, Count#5], true, 10000, StorageLevel(disk, memory, deserialized, 1 replicas), `stateNames`
# vs direct file scan in the second example

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("DataFrame Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    stateNames.createOrReplaceTempView("stateNames")
    spark.sql("CACHE TABLE stateNames")
    # Get full list of boy names
    sqlQuery = spark.sql("SELECT DISTINCT Name FROM stateNames WHERE Gender = 'M' ORDER BY Name")
    sqlQuery.show()
    sqlQuery.explain(extended=True)

    spark.sql("UNCACHE TABLE stateNames")
    sqlQuery2 = spark.sql("SELECT DISTINCT Name FROM stateNames WHERE Gender = 'M' ORDER BY Name")
    sqlQuery2.show()
    sqlQuery2.explain(extended=True)

    spark.stop()
