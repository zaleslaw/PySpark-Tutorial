from __future__ import print_function

# $example on$
from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.linalg import Vectors
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MaxAbsScalerExample")\
        .getOrCreate()

    # $example on$
    dataFrame = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.1, -8.0]),),
        (1, Vectors.dense([2.0, 1.0, -4.0]),),
        (2, Vectors.dense([4.0, 10.0, 8.0]),)
    ], ["id", "features"])

    scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")

    # Compute summary statistics and generate MaxAbsScalerModel
    scalerModel = scaler.fit(dataFrame)

    # rescale each feature to range [-1, 1].
    scaledData = scalerModel.transform(dataFrame)

    scaledData.select("features", "scaledFeatures").show()
    # $example off$

    spark.stop()