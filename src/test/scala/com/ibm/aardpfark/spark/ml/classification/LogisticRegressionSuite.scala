package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.ProbClassifierResult

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}

class LogisticRegressionSuite extends SparkClassifierPFASuiteBase[ProbClassifierResult] {
  import spark.implicits._

  def getOutput(df: DataFrame) = {
    df.select(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).map
    {
      case Row(p: Double, raw: Vector, pr: Vector) => (p, raw.toArray, pr.toArray)
    }.toDF(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).toJSON.collect()
  }

  val binaryData = spark.read.format("libsvm").load("data/sample_libsvm_data.txt")
  val multiData = spark.read.format("libsvm").load("data/sample_multiclass_classification_data.txt")

  val clf = new LogisticRegression()

  override val sparkTransformer = clf.fit(binaryData)
  val result = sparkTransformer.transform(binaryData)
  override val input = withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
  override val expectedOutput = getOutput(result)

  // Additional tests
  test("LogisticRegression w/o fitIntercept") {
    val sparkTransformer = clf.setFitIntercept(false).fit(binaryData)
    val result = sparkTransformer.transform(binaryData)
    val expectedOutput = getOutput(result)

    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("LogisticRegression w/ non-default threshold") {
    val sparkTransformer = clf.setThreshold(0.0).fit(binaryData)
    val result = sparkTransformer.transform(binaryData)
    val expectedOutput = getOutput(result)

    parityTest(sparkTransformer, input, expectedOutput)

    val sparkTransformer2 = clf.setThreshold(1.0).fit(binaryData)
    val result2 = sparkTransformer2.transform(binaryData)
    val expectedOutput2 = getOutput(result2)

    parityTest(sparkTransformer2, input, expectedOutput2)
  }

  test("MLOR w/ intercept") {
    val sparkTransformer = clf.fit(multiData)
    val result = sparkTransformer.transform(multiData)
    val input =  withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
    val expectedOutput = getOutput(result)

    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("MLOR w/o intercept") {
    val sparkTransformer = clf.setFitIntercept(false).fit(multiData)
    val result = sparkTransformer.transform(multiData)
    val input =  withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
    val expectedOutput = getOutput(result)

    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("MLOR w/ thresholds") {
    val sparkTransformer = clf.setThresholds(Array(0.1, 0.6, 0.3)).fit(multiData)
    val result = sparkTransformer.transform(multiData)
    val input =  withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
    val expectedOutput = getOutput(result)

    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("MLOR w/ thresholds - one zero") {
    val sparkTransformer = clf.setThresholds(Array(0.0, 0.6, 0.3)).fit(multiData)
    val result = sparkTransformer.transform(multiData)
    val input =  withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
    val expectedOutput = getOutput(result)

    parityTest(sparkTransformer, input, expectedOutput)
  }

}
