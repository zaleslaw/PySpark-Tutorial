from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Average") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.stop()
