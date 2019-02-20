from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list


def getSparkSession():
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Animals") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def readAnimalDataset(spark):
    animals = spark.read \
        .option("inferSchema", "true") \
        .option("charset", "windows-1251") \
        .option("header", "true") \
        .csv("/home/zaleslaw/data/cyr_animals.csv") \

    animals.show() \

    classNames = spark.read \
        .option("inferSchema", "true") \
        .option("charset", "windows-1251") \
        .option("header", "true") \
        .csv("/home/zaleslaw/data/cyr_class.csv") \

    classNames.show(truncate=False) \

    animalsWithClassTypeNames = animals.join(classNames, animals.type == classNames.Class_Number) \

    return animalsWithClassTypeNames


if __name__ == "__main__":
    spark = getSparkSession()
    animals = readAnimalDataset(spark)
    animals.show()

    # Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler

    assembler = VectorAssembler(inputCols=["hair", "milk", "eggs"], outputCol="features")
        # "hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"


    # Step - 2: Transform dataframe to vectorized dataframe
    vectorizedDF = assembler.transform(animals).select("features", "cyr_name", "Cyr_Class_Type")

    for i in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 40]:
        print("Clusterize with " + str(i) + " clusters")

        # Step - 3: Train model
        kMeansTrainer = KMeans(featuresCol="features", predictionCol="cluster", k=i)
        model = kMeansTrainer.fit(vectorizedDF)
        # Step - 4: Print out the sum of squared distances of points to their nearest center
        SSE = model.computeCost(vectorizedDF)
        print("Sum of Squared Errors = " + str(SSE))

        # Step - 5: Print out cluster centers
        print("Cluster Centers: ")
        print(model.clusterCenters())

        print("Real clusters and predicted clusters")
        predictions = model.summary.predictions

        # Step - 6: Print out predicted and real classes

        print("Predicted classes")

        predictions \
            .select("cyr_name", "cluster") \
            .groupBy("cluster") \
            .agg(collect_list("cyr_name")) \
            .orderBy("cluster") \
            .show(int(predictions.count()), truncate=False)

    print("Real classes")
    vectorizedDF \
        .select("cyr_name", "Cyr_Class_Type") \
        .groupBy("Cyr_Class_Type") \
        .agg(collect_list("cyr_name")).show(int(vectorizedDF.count()), truncate=False) \

    spark.stop()
