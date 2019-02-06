from pyspark.ml.classification import DecisionTreeClassifier, LinearSVC
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import collect_list, udf
from pyspark.sql.types import StringType


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
        .csv("/home/zaleslaw/data/cyr_binarized_animals.csv")

    animals.show()

    return animals


def funcCheckClasses(type: float, prediction: float):
    return str("") if (type == prediction) else str("ERROR")


def enrichPredictions(rawPredictions):
    checkClasses = udf(lambda type, prediction: funcCheckClasses(type, prediction), StringType())

    select = rawPredictions.select(rawPredictions["cyr_name"].alias("Name"),
                                   rawPredictions["eatable"],
                                   rawPredictions["prediction"])
    select__with_column = select.withColumn("Error",
                                            checkClasses(select["eatable"], select["prediction"]))
    predictions = select__with_column \
        .orderBy(select__with_column["Error"].desc())

    return predictions


# We changed classifier and
if __name__ == "__main__":
    spark = getSparkSession()
    animals = readAnimalDataset(spark)
    animals.show()

    # Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler

    assembler = VectorAssembler(
        inputCols=["hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone",
                   "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"],
        outputCol="features")

    # Step - 2: Transform dataframe to vectorized dataframe
    output = assembler.transform(animals).select("features", "eatable", "cyr_name")

    output.cache()

    # Step - 3: Set up the LinearSVC Classifier
    trainer = DecisionTreeClassifier(labelCol="eatable", featuresCol="features")

    # Step - 4: Train the model
    model = trainer.fit(output)

    print(model.toDebugString)

    rawPredictions = model.transform(output)

    predictions = enrichPredictions(rawPredictions)

    predictions.show(100)

    # Step - 5: Evaluate prediction
    evaluator = BinaryClassificationEvaluator(labelCol="eatable", rawPredictionCol="prediction")

    # Step - 6: Calculate ROC AUC
    rocAuc = evaluator.evaluate(rawPredictions)
    print("ROC_AUC = %g " % rocAuc)

    spark.stop()
