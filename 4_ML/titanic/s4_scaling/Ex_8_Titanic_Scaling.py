from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, Imputer, StringIndexer, MinMaxScaler, Normalizer
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

class Printer(Transformer):
    def __init__(self, message: str):
        super(Printer, self).__init__()
        self.message = message

    def _transform(self, dataset: DataFrame) -> DataFrame:
        print(self.message)
        dataset.show(truncate=False)
        dataset.printSchema()
        return dataset


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
        outputCol="unscaled_features")

    # Step - 5: Define Scaler
    scaler = MinMaxScaler(inputCol="unscaled_features", outputCol="unnorm_features")

    # Step - 6: Define Normalizer
    normalizer = Normalizer(p=1.0, inputCol="unnorm_features", outputCol="features")

    # Step - 7: Set up the Decision Tree Classifier
    trainer = DecisionTreeClassifier(labelCol="survived", featuresCol="features")

    # Step - 8: Build the Pipeline
    pipeline = Pipeline(stages=[sexIndexer,
                                embarkedIndexer,
                                imputer,
                                assembler,
                                Printer("After assembler"),
                                scaler,
                                Printer("After scaling"),
                                normalizer,
                                Printer("After normalizing"),
                                trainer])

    # Step - 9: Train the model
    model = pipeline.fit(passengers)

    # Step - 10: Predict with the model
    rawPredictions = model.transform(passengers)

    # Step - 11: Evaluate prediction
    evaluator = MulticlassClassificationEvaluator(labelCol="survived", predictionCol="prediction",
                                                  metricName="accuracy")

    # Step - 12: Calculate accuracy
    accuracy = evaluator.evaluate(rawPredictions)
    print("Test Error = %g " % (1.0 - accuracy))

    spark.stop()
