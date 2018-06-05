package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.ProbClassifierResult

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.linalg.{Vector, Vectors}

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

class NaiveBayesSuite extends SparkClassifierPFASuiteBase[ProbClassifierResult] {

  import spark.implicits._

  val inputPath = "data/sample_multiclass_classification_data.txt"
  val dataset = spark.read.format("libsvm").load(inputPath)
  val multinomialData = dataset
    .as[(Double, Vector)]
    .map { case (label, vector) =>
      val nonZeroVector = Vectors.dense(vector.toArray.map(math.max(0.0, _)))
      (label, nonZeroVector)
    }.toDF("label", "features")

  val multinomialDataBinary = multinomialData.select(
    when(col("label") >= 1, 1.0).otherwise(0.0).alias("label"), col("features")
  )

  val bernoulliData = dataset
    .as[(Double, Vector)]
    .map { case (label, vector) =>
      val binaryData = vector.toArray.map {
        case e if e > 0.0 =>
          1.0
        case e if e <= 0.0 =>
          0.0
      }
      (label, Vectors.dense(binaryData))
    }.toDF("label", "features")

  val bernoulliDataBinary = bernoulliData.select(
    when(col("label") >= 1, 1.0).otherwise(0.0).alias("label"), col("features")
  )

  val clf = new NaiveBayes()

  override val sparkTransformer = clf.fit(multinomialData)
  val result = sparkTransformer.transform(multinomialData)
  override val input = withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
  override val expectedOutput = result.select(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).map {
    case Row(p: Double, raw: Vector, pr: Vector) => (p, raw.toArray, pr.toArray)
  }.toDF(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).toJSON.collect()

  // Additional tests
  test("Multinomial model binary classification") {
    val sparkTransformer = clf.fit(multinomialDataBinary)
    val result = sparkTransformer.transform(multinomialDataBinary)
    val input = withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
    val expectedOutput = result.select(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).map {
      case Row(p: Double, raw: Vector, pr: Vector) => (p, raw.toArray, pr.toArray)
    }.toDF(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).toJSON.collect()
    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("Bernoulli model") {
    val sparkTransformer = clf.setModelType("bernoulli").fit(bernoulliData)
    val result = sparkTransformer.transform(bernoulliData)
    val input = withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
    val expectedOutput = result.select(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).map {
      case Row(p: Double, raw: Vector, pr: Vector) => (p, raw.toArray, pr.toArray)
    }.toDF(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).toJSON.collect()
    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("Bernoulli model binary classification") {
    val sparkTransformer = clf.setModelType("bernoulli").fit(bernoulliDataBinary)
    val result = sparkTransformer.transform(bernoulliDataBinary)
    val input = withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
    val expectedOutput = result.select(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).map {
      case Row(p: Double, raw: Vector, pr: Vector) => (p, raw.toArray, pr.toArray)
    }.toDF(clf.getPredictionCol, clf.getRawPredictionCol, clf.getProbabilityCol).toJSON.collect()
    parityTest(sparkTransformer, input, expectedOutput)
  }

}
