from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Average") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    salaries = [
        "John 1900 January",
        "Mary 2000 January",
        "John 1800 February",
        "John 1000 March",
        "Mary 1500 February",
        "Mary 2900 March"
    ]

    rdd = spark.sparkContext.parallelize(salaries).map(lambda s: s.split(" "))

    res = rdd.map(lambda x: (x[0], (float(x[1]), 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .map(lambda e: (e[0], e[1][0] / e[1][1])) \
        .collect()
    print(res)

    spark.stop()
