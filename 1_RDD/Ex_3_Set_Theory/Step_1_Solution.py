from __future__ import print_function
from pyspark.sql import SparkSession

# Set theory in Spark

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Set_theory") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    jvmLanguages = sc.parallelize(["Scala", "Java", "Groovy", "Kotlin", "Ceylon"])
    functionalLanguages = sc.parallelize(["Scala", "Kotlin", "JavaScript", "Haskell"])
    webLanguages = sc.parallelize(["PHP", "Ruby", "Perl", "PHP", "JavaScript"])

    print("----Distinct----")
    distinctLangs = webLanguages.union(jvmLanguages).distinct()
    print(distinctLangs.toDebugString())
    print(distinctLangs.collect())

    print("----Intersection----")
    intersection = jvmLanguages.intersection(functionalLanguages)
    print(intersection.toDebugString())
    print(intersection.collect())

    print("----Substract----")
    substraction = webLanguages.distinct().subtract(functionalLanguages)
    print(substraction.toDebugString())
    print(substraction.collect())

    print("----Cartesian----")
    cartestian = webLanguages.distinct().cartesian(jvmLanguages)
    print(cartestian.toDebugString())
    print(cartestian.collect())

    spark.stop()
