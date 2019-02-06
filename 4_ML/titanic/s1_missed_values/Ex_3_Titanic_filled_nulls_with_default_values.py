from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession


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

    passengers.show()

    return passengers


if __name__ == "__main__":
    spark = getSparkSession()
    passengers = readPassengers(spark)

    # Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler

    assembler = VectorAssembler(
        inputCols=["pclass", "sibsp", "parch"],
        outputCol="features")

    replacements = {'pclass': 1, 'sibsp': 0, 'parch': 0}

    # Step - 2: Transform dataframe to vectorized dataframe
    passengersWithFilledEmptyValues = passengers.select("pclass", "sibsp", "parch", "survived").na.fill(replacements)
    passengersWithFilledEmptyValues.show() # look at first row

    output = assembler.transform(passengersWithFilledEmptyValues ).select("features", "survived")  # <============== drop row if it has nulls/NaNs in the next list of columns)
    output.show()

    # Step - 3: Set up the Decision Tree Classifier
    trainer = DecisionTreeClassifier(labelCol="survived", featuresCol="features")

    # Step - 4: Train the model
    model = trainer.fit(output)

    # Step - 5: Predict with the model
    rawPredictions = model.transform(output)

    # Step - 6: Evaluate prediction
    evaluator = MulticlassClassificationEvaluator(labelCol="survived", predictionCol="prediction",
                                                  metricName="accuracy")

    # Step - 7: Calculate accuracy
    accuracy = evaluator.evaluate(rawPredictions)
    print("Test Error = %g " % (1.0 - accuracy))

    # Step - 8: Print out the model
    print(model.toDebugString)

    spark.stop()
