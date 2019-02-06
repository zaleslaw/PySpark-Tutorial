from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, Imputer, StringIndexer
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


#  Add two features: "sex" and "embarked". They are presented as sets of string. Accuracy = 0,194
#  Old columns should be dropped from the dataset to use imputer
#  The first row in imputed dataset is filled with the special values
if __name__ == "__main__":
    spark = getSparkSession()
    passengers = readPassengersWithCastingToDoubles(spark).select("survived", "pclass", "sibsp", "parch", "sex",
                                                                  "embarked")

    # Step - 1: Define the Indexer for the column "sex"
    sexIndexer = StringIndexer(inputCol="sex",
                               outputCol="sexIndexed",
                               handleInvalid="keep")  # special mode to create special double value for null values

    passengersWithIndexedSex = sexIndexer.fit(passengers).transform(passengers)

    # Step - 2: Define the Indexer for the column "embarked"
    embarkedIndexer = StringIndexer(inputCol="embarked",
                                    outputCol="embarkedIndexed",
                                    handleInvalid="keep")  # special mode to create special double value for null valuesnew StringIndexer()

    passengersWithIndexedCategorialFeatures = embarkedIndexer \
        .fit(passengersWithIndexedSex) \
        .transform(passengersWithIndexedSex) \
        .drop("sex", "embarked")  # <============== drop columns to use Imputer

    passengersWithIndexedCategorialFeatures.show()
    passengersWithIndexedCategorialFeatures.printSchema()

    # Step - 3: Define strategy and new column names for Imputer transformation
    imputer = Imputer(strategy="mean", inputCols=["pclass", "sibsp", "parch", "sexIndexed", "embarkedIndexed"],
                      outputCols=["pclass_imputed", "sibsp_imputed", "parch_imputed", "sexIndexed_imputed", "embarkedIndexed_imputed"])

    # Step - 4: Make Vectors from dataframe's columns using special Vector Assmebler
    assembler = VectorAssembler(
        inputCols=["pclass_imputed", "sibsp_imputed", "parch_imputed", "sexIndexed_imputed", "embarkedIndexed_imputed"],
        outputCol="features")

    # Step - 5: Transform the dataset with the Imputer
    passengersWithFilledEmptyValues = imputer.fit(passengersWithIndexedCategorialFeatures).transform(passengersWithIndexedCategorialFeatures)

    print("Special values created by imputer and encoder in the first row")
    passengersWithFilledEmptyValues.show(1)

    passengersWithFilledEmptyValues.show()  # look at first row

    #  Step - 6: Transform dataframe to vectorized dataframe
    output = assembler.transform(passengersWithFilledEmptyValues).select("features",
                                                                         "survived")  # <============== drop row if it has nulls/NaNs in the next list of columns)
    output.show()

    # Step - 7: Set up the Decision Tree Classifier
    trainer = DecisionTreeClassifier(labelCol="survived", featuresCol="features")

    # Step - 8: Train the model
    model = trainer.fit(output)

    # Step - 9: Predict with the model
    rawPredictions = model.transform(output)

    # Step - 10: Evaluate prediction
    evaluator = MulticlassClassificationEvaluator(labelCol="survived", predictionCol="prediction",
                                                  metricName="accuracy")

    # Step - 11: Calculate accuracy
    accuracy = evaluator.evaluate(rawPredictions)
    print("Test Error = %g " % (1.0 - accuracy))

    # Step - 12: Print out the model
    print(model.toDebugString)

    spark.stop()
