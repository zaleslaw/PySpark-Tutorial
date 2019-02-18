import os

from pyspark.sql import SparkSession

# Create large RDD, convert to DF, compare stolen size in storage.
from pyspark.storagelevel import StorageLevel

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("DataFrame Intro") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    intSeq = []
    for i in range(10000000):
        intSeq.append(i)

    intRDD = sc.parallelize(intSeq)
    print(intRDD.persist(StorageLevel.MEMORY_ONLY).count())
    df = intRDD.map(lambda x: (x,)).toDF()
    print(df.persist(StorageLevel.MEMORY_ONLY).count())
    df.show()

    spark.stop()
