package com.ibm.aardpfark.spark.ml.regression

import com.ibm.aardpfark.pfa.PredictorResult
import org.apache.spark.ml.regression.GBTRegressor

class GBTRegressionSuite extends SparkRegressorPFASuiteBase[PredictorResult] {

  val data = spark.read.format("libsvm").load(inputPath)
  val dt = new GBTRegressor()
    .setMaxDepth(5)
  override val sparkTransformer = dt.fit(data)

  val result = sparkTransformer.transform(data)
  override val input = withColumnAsArray(result, dt.getFeaturesCol).toJSON.collect()
  override val expectedOutput = result.select(dt.getPredictionCol).toJSON.collect()

}
