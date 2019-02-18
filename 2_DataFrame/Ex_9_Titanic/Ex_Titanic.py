from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import avg, count


def getSparkSession():
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Titanic") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def readPassengers(spark):
    passengers = spark.read \
        .option("delimiter", ";") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .csv("/home/zaleslaw/data/titanic.csv")

    passengers.printSchema()

    return passengers


if __name__ == "__main__":
    spark = getSparkSession()
    passengers = readPassengers(spark)
    passengers.show(15)

    # Step-1: Find amount of passengers in each class
    passengers.groupBy("pclass").count().show()

    # Step-2: The cabin with the maximum amount of survived passengers (drop null)
    print(passengers.where("cabin is not null").groupBy("cabin").sum("survived").orderBy("sum(survived)", ascending=False).take(1))

    # Step-3: Avg age in each group
    passengers.withColumn("age", passengers["age"].cast(DoubleType())).groupBy("sex", "pclass", "survived").agg(avg("age"), count("age")).orderBy("avg(age)").show()

    spark.stop()
