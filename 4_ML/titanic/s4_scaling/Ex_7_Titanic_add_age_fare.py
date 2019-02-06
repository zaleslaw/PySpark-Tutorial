from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, Imputer, StringIndexer
from pyspark.sql import SparkSession, DataFrame
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
        .withColumn("parch", passengers["parch"].cast(DoubleType())) \
        .withColumn("age", passengers["age"].cast(DoubleType())) \
        .withColumn("fare", passengers["fare"].cast(DoubleType())) \

    castedPassengers.printSchema()

    castedPassengers.show()

    return castedPassengers


class DropSex(Transformer):
    def __init__(self):
        super(DropSex, self).__init__()

    def _transform(self, df: DataFrame) -> DataFrame:
        result = df.drop("sex", "embarked")  # <============== drop columns to use Imputer
        print("DropSex is working")
        result.show()
        result.printSchema()
        return result


# Add two features: age and fare. Accuracy = 0.174942
if __name__ == "__main__":
    spark = getSparkSession()
    passengers = readPassengersWithCastingToDoubles(spark).select("survived", "pclass", "sibsp", "parch", "sex",
                                                                  "embarked", "age", "fare")

    # Step - 1: Define the Indexer for the column "sex"
    sexIndexer = StringIndexer(inputCol="sex",
                               outputCol="sexIndexed",
                               handleInvalid="keep")  # special mode to create special double value for null values

    # Step - 2: Define the Indexer for the column "embarked"
    embarkedIndexer = StringIndexer(inputCol="embarked",
                                    outputCol="embarkedIndexed",
                                    handleInvalid="keep")  # special mode to create special double value for null valuesnew StringIndexer()

    # Step - 3: Define strategy and new column names for Imputer transformation
    imputer = Imputer(strategy="mean", inputCols=["pclass", "sibsp", "parch", "sexIndexed", "embarkedIndexed", "age", "fare"],
                      outputCols=["pclass_imputed", "sibsp_imputed", "parch_imputed", "sexIndexed_imputed",
                                  "embarkedIndexed_imputed", "age_imputed", "fare_imputed"])

    # Step - 4: Make Vectors from dataframe's columns using special Vector Assmebler
    assembler = VectorAssembler(
        inputCols=["pclass_imputed", "sibsp_imputed", "parch_imputed", "sexIndexed_imputed", "embarkedIndexed_imputed",
                   "age_imputed", "fare_imputed"],
        outputCol="features")

    # Step - 5: Set up the Decision Tree Classifier
    trainer = DecisionTreeClassifier(labelCol="survived", featuresCol="features")

    # Step - 6: Build the Pipeline
    pipeline = Pipeline(stages=[sexIndexer, embarkedIndexer, DropSex(), imputer, assembler, trainer])

    # Step - 7: Train the model
    model = pipeline.fit(passengers)

    # Step - 8: Predict with the model
    rawPredictions = model.transform(passengers)

    # Step - 9: Evaluate prediction
    evaluator = MulticlassClassificationEvaluator(labelCol="survived", predictionCol="prediction",
                                                  metricName="accuracy")

    # Step - 10: Calculate accuracy
    accuracy = evaluator.evaluate(rawPredictions)
    print("Test Error = %g " % (1.0 - accuracy))

    spark.stop()
