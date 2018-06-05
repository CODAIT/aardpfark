package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{ScalerResult, Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class NormalizerSuite extends SparkFeaturePFASuiteBase[ScalerResult] {

  implicit val enc = ExpressionEncoder[Vector]()

  val inputPath = "data/sample_lda_libsvm_data.txt"
  val dataset = spark.read.format("libsvm").load(inputPath)

  val scaler = new Normalizer()
    .setInputCol("features")
    .setOutputCol("scaled")

  override val sparkTransformer = scaler

  val result = scaler.transform(dataset)
  override val input = withColumnAsArray(result, scaler.getInputCol).toJSON.collect()
  override val expectedOutput = withColumnAsArray(result, scaler.getOutputCol).toJSON.collect()

  test("Normalizer with P = 1") {
    val sparkTransformer = scaler.setP(1.0)
    val result = sparkTransformer.transform(dataset)
    val expectedOutput = withColumnAsArray(result, scaler.getOutputCol).toJSON.collect()
    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("Normalizer with P = positive infinity"){
    val sparkTransformer = scaler.setP(Double.PositiveInfinity)
    val result = sparkTransformer.transform(dataset)
    val expectedOutput = withColumnAsArray(result, scaler.getOutputCol).toJSON.collect()
    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("Normalizer with P = 3") {
    val sparkTransformer = scaler.setP(3.0)
    val result = sparkTransformer.transform(dataset)
    val expectedOutput = withColumnAsArray(result, scaler.getOutputCol).toJSON.collect()
    parityTest(sparkTransformer, input, expectedOutput)
  }

}