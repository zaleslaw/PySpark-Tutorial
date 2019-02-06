from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, Imputer
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType


def getSparkSession():
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("Titanic") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def readPassengersWithCastingToDoubles(spark):
    passengers = spark.read \
        .option("delimiter", ";") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .csv("/home/zaleslaw/data/titanic.csv")

    castedPassengers = passengers.withColumn("survived", passengers["survived"].cast(DoubleType())) \
        .withColumn("pclass", passengers["pclass"].cast(DoubleType())) \
        .withColumn("sibsp", passengers["sibsp"].cast(DoubleType())) \
        .withColumn("parch", passengers["parch"].cast(DoubleType()))

    castedPassengers.printSchema()

    castedPassengers.show()

    return castedPassengers


# look into new readPassengers function
if __name__ == "__main__":
    spark = getSparkSession()
    passengers = readPassengersWithCastingToDoubles(spark).select("survived", "pclass", "sibsp", "parch")

    # Step - 1: Define strategy and new column names for Imputer transformation
    imputer = Imputer(strategy="mean", inputCols=["pclass", "sibsp", "parch"],
                      outputCols=["pclass_imputed", "sibsp_imputed", "parch_imputed"])

    # Step - 2: Make Vectors from dataframe's columns using special Vector Assmebler
    assembler = VectorAssembler(
        inputCols=["pclass_imputed", "sibsp_imputed", "parch_imputed"],
        outputCol="features")

    # Step - 3: Transform the dataset with the Imputer
    passengersWithFilledEmptyValues = imputer.fit(passengers).transform(passengers)
    passengersWithFilledEmptyValues.show()  # look at first row

    #  Step - 4: Transform dataframe to vectorized dataframe
    output = assembler.transform(passengersWithFilledEmptyValues).select("features",
                                                                         "survived")  # <============== drop row if it has nulls/NaNs in the next list of columns)
    output.show()

    # Step - 5: Set up the Decision Tree Classifier
    trainer = DecisionTreeClassifier(labelCol="survived", featuresCol="features")

    # Step - 6: Train the model
    model = trainer.fit(output)

    # Step - 7: Predict with the model
    rawPredictions = model.transform(output)

    # Step - 8: Evaluate prediction
    evaluator = MulticlassClassificationEvaluator(labelCol="survived", predictionCol="prediction",
                                                  metricName="accuracy")

    # Step - 9: Calculate accuracy
    accuracy = evaluator.evaluate(rawPredictions)
    print("Test Error = %g " % (1.0 - accuracy))

    # Step - 10: Print out the model
    print(model.toDebugString)

    spark.stop()
