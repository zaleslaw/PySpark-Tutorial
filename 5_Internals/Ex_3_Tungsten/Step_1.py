from pyspark.sql import SparkSession

# Let's make simple query
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("DataFrame Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    query = stateNames.where("Year = 2014")
    query.show()
    print(query.explain(extended=True))

    spark.stop()
