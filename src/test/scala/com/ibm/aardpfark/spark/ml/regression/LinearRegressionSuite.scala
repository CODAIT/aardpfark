package com.ibm.aardpfark.spark.ml.regression

import com.ibm.aardpfark.pfa.PredictorResult
import org.apache.spark.ml.regression.LinearRegression

class LinearRegressionSuite extends SparkRegressorPFASuiteBase[PredictorResult] {

  val dataset = spark.read.format("libsvm").load(inputPath)
  val lr = new LinearRegression()
  override val sparkTransformer = lr.fit(dataset)
  val result = sparkTransformer.transform(dataset)

  override val input = withColumnAsArray(result, lr.getFeaturesCol).toJSON.collect()
  override val expectedOutput = result.select(lr.getPredictionCol).toJSON.collect()

  // Additional tests
  test("LinearRegression w/o fitIntercept") {
    val sparkTransformer = lr.setFitIntercept(false).fit(dataset)
    val result = sparkTransformer.transform(dataset)
    val expectedOutput = result.select(lr.getPredictionCol).toJSON.collect()

    parityTest(sparkTransformer, input, expectedOutput)
  }

}
