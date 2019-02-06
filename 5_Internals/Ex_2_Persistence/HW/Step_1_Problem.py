from pyspark.sql import SparkSession

# Cache data and see new plan
#  InMemoryTableScan [Id#0, Name#1, Year#2, Gender#3, State#4, Count#5], [isnotnull(Year#2), (Year#2 = 2014)]
#       +- InMemoryRelation [Id#0, Name#1, Year#2, Gender#3, State#4, Count#5], true, 10000, StorageLevel(memory, 1 replicas)
from pyspark.storagelevel import StorageLevel

# strange effect in variable usage
# the same plan after persisting
# try to play with Storage Levels, check on <hostname>:4040/storage/ that rdd/df was cashed
# discuss hypothesis
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

    query.show()
    print(query.explain(extended=True))

    spark.stop()
