from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Cache different dataframes and look into the execution plan changes.
# What interesting could be found there?
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Popular girls names") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    filteredStateNames = stateNames \
        .where("Year=1945 and Gender='M'") \
        .select("Name", "State", "Count")

    stateAndCount = filteredStateNames \
        .groupBy("State") \
        .agg(f.max("Count").alias("max"))

    cond = [stateAndCount.max == filteredStateNames.Count, stateAndCount.State == filteredStateNames.State]
    stateAndName = filteredStateNames \
        .join(stateAndCount, cond) \
        .select(filteredStateNames["State"], filteredStateNames["Name"].alias("name")) \
        .orderBy(filteredStateNames["State"].desc(), filteredStateNames["Count"].desc())

    stateAndName.printSchema()
    stateAndName.show(100)

    stateAndName.explain()
    stateAndName.explain(extended=True)

    spark.stop()
