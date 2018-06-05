package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.ClassifierResult
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class LinearSVCSuite extends SparkClassifierPFASuiteBase[ClassifierResult] {

  val inputPath = "data/sample_libsvm_data.txt"
  val dataset = spark.read.format("libsvm").load(inputPath)
  val clf = new LinearSVC()
  override val sparkTransformer = clf.fit(dataset)
  import spark.implicits._

  implicit val mapEncoder = ExpressionEncoder[Map[String, Double]]()
  val result = sparkTransformer.transform(dataset)
  override val input = withColumnAsArray(result, clf.getFeaturesCol).toJSON.collect()
  override val expectedOutput = result.select(clf.getPredictionCol, clf.getRawPredictionCol).map {
    case Row(p: Double, raw: Vector) => (p, raw.toArray)
  }.toDF(clf.getPredictionCol, clf.getRawPredictionCol).toJSON.collect()

  // Additional tests
  test("LinearSVC w/o fitIntercept") {
    val sparkTransformer = clf.setFitIntercept(false).fit(dataset)
    val result = sparkTransformer.transform(dataset)
    val expectedOutput = result.select(clf.getPredictionCol, clf.getRawPredictionCol).map {
      case Row(p: Double, raw: Vector) => (p, raw.toArray)
    }.toDF(clf.getPredictionCol, clf.getRawPredictionCol).toJSON.collect()

    parityTest(sparkTransformer, input, expectedOutput)
  }
}
