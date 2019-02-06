from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
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
        .csv("/home/zaleslaw/data/cyr_animals.csv")

    animals.show()

    classNames = spark.read \
        .option("inferSchema", "true") \
        .option("charset", "windows-1251") \
        .option("header", "true") \
        .csv("/home/zaleslaw/data/cyr_class.csv")

    classNames.show(truncate=False)

    animalsWithClassTypeNames = animals.join(classNames, animals.type == classNames.Class_Number)

    return (classNames, animalsWithClassTypeNames)


def funcCheckClasses(type: str, prediction: str):
    return str("") if (type == prediction) else str("ERROR")


def enrichPredictions(spark, classNames, rawPredictions):
    prClassNames = classNames.select(classNames["Class_Number"].alias("pr_class_id"),
                                     classNames["Cyr_Class_Type"].alias("pr_class_type"))
    enrichedPredictions = rawPredictions.join(prClassNames, rawPredictions.prediction == prClassNames.pr_class_id)

    checkClasses = udf(lambda type, prediction: funcCheckClasses(type, prediction), StringType())

    select = enrichedPredictions.select(enrichedPredictions["name"], enrichedPredictions["cyr_name"].alias("Name"),
                                        enrichedPredictions["Cyr_Class_Type"].alias("Real_class_type"),
                                        enrichedPredictions["pr_class_type"].alias("Predicted_class_type"))
    select__with_column = select.withColumn("Error", checkClasses(select["Real_class_type"], select["Predicted_class_type"]))
    predictions = select__with_column \
        .orderBy(select__with_column["Error"].desc())

    return predictions


if __name__ == "__main__":
    spark = getSparkSession()
    classNames, animals = readAnimalDataset(spark)
    animals.show()

    # Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler

    assembler = VectorAssembler(
        inputCols=["hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone",
                   "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"],
        outputCol="features")

    # Step - 2: Transform dataframe to vectorized dataframe
    output = assembler.transform(animals).select("features", "name", "type", "cyr_name", "Cyr_Class_Type")

    # Step - 3: Set up the Decision Tree Classifier
    trainer = DecisionTreeClassifier(labelCol="type", featuresCol="features")

    # Step - 4: Train the model
    model = trainer.fit(output)

    rawPredictions = model.transform(output)

    predictions = enrichPredictions(spark, classNames, rawPredictions)

    predictions.show(100)

    # Step - 5: Evaluate prediction
    evaluator = MulticlassClassificationEvaluator(labelCol="type", predictionCol="prediction",
                                                  metricName="accuracy")

    # Step - 6: Calculate accuracy
    accuracy = evaluator.evaluate(rawPredictions)
    print("Test Error = %g " % (1.0 - accuracy))

    # Step - 7: Print out the model
    print(model.toDebugString)

    spark.stop()
