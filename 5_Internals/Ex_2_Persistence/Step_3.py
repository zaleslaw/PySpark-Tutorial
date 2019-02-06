from pyspark.sql import SparkSession

# Unpersist data and see the old plan
from pyspark.storagelevel import StorageLevel

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

    print(stateNames.storageLevel)
    stateNames.persist(storageLevel=StorageLevel.MEMORY_ONLY)
    print(stateNames.storageLevel)

    query2 = stateNames.where("Year = 2014")
    query2.show()
    print(query2.explain(extended=True))

    stateNames.unpersist()
    print(stateNames.storageLevel)

    query3 = stateNames.where("Year = 2014")
    query3.show()
    print(query3.explain(extended=True))

    spark.stop()
