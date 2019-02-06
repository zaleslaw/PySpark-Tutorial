from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, Imputer, StringIndexer, MinMaxScaler, Normalizer, PolynomialExpansion, \
    PCA, RegexTokenizer, StopWordsRemover, HashingTF
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
        .withColumn("fare", passengers["fare"].cast(DoubleType()))

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


# Let's extract text features from "name" column. Divide each string on separate "names" and build hashingTF model
#  Remove stop words with StopWordsRemover
#  Select features with PCA. Accuracy = 0.1428 and reduced with increasing of PCA from 50 to 1000
#  Increasing amount of features with decreasing of accuracy is an example of overfit
if __name__ == "__main__":
    spark = getSparkSession()
    passengers = readPassengersWithCastingToDoubles(spark).select("survived", "pclass", "sibsp", "parch", "sex",
                                                                  "embarked", "age", "fare", "name")

    training, test = passengers.randomSplit([0.7, 0.3], seed=12345)
    training.cache()
    test.cache()

    regexTokenizer = RegexTokenizer(gaps=False, pattern="\\w+", inputCol="name", outputCol="name_parts",
                                    toLowercase=True)

    stopWords = ["mr", "mrs", "miss", "master", "jr", "j", "c", "d"]

    remover = StopWordsRemover(inputCol="name_parts", outputCol="filtered_name_parts", stopWords=stopWords)

    hashingTF = HashingTF(numFeatures=1000, inputCol="filtered_name_parts", outputCol="text_features")

    sexIndexer = StringIndexer(inputCol="sex",
                               outputCol="sexIndexed",
                               handleInvalid="keep")

    embarkedIndexer = StringIndexer(inputCol="embarked",
                                    outputCol="embarkedIndexed",
                                    handleInvalid="keep")

    imputer = Imputer(strategy="mean",
                      inputCols=["pclass", "sibsp", "parch", "sexIndexed", "embarkedIndexed", "age", "fare"],
                      outputCols=["pclass_imputed", "sibsp_imputed", "parch_imputed", "sexIndexed_imputed",
                                  "embarkedIndexed_imputed", "age_imputed", "fare_imputed"])

    assembler = VectorAssembler(
        inputCols=["pclass_imputed", "sibsp_imputed", "parch_imputed", "sexIndexed_imputed", "embarkedIndexed_imputed",
                   "age_imputed", "fare_imputed"],
        outputCol="unscaled_features")

    polyExpansion = PolynomialExpansion(degree=2, inputCol="unscaled_features", outputCol="polyFeatures")

    # We should join together text features and number features into one vector
    assembler2 = VectorAssembler(
        inputCols=["polyFeatures", "text_features"],
        outputCol="joinedFeatures")

    pca = PCA(k=15, inputCol="joinedFeatures", outputCol="features")

    trainer = DecisionTreeClassifier(labelCol="survived", featuresCol="features")

    pipeline = Pipeline(stages=[regexTokenizer,
                                remover,
                                hashingTF,
                                sexIndexer,
                                embarkedIndexer,
                                imputer,
                                assembler,
                                polyExpansion,
                                assembler2,
                                Printer("After Final Assembling"),
                                pca,
                                Printer("After PCA"),
                                trainer])

    model = pipeline.fit(training)

    rawPredictions = model.transform(test)

    evaluator = MulticlassClassificationEvaluator(labelCol="survived", predictionCol="prediction",
                                                  metricName="accuracy")

    accuracy = evaluator.evaluate(rawPredictions)
    print("Test Error = %g " % (1.0 - accuracy))

    spark.stop()
