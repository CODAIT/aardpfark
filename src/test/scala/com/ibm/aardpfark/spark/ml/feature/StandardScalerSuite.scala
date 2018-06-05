package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{ScalerResult, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class StandardScalerSuite extends SparkFeaturePFASuiteBase[ScalerResult] {

  implicit val enc = ExpressionEncoder[Vector]()

  val inputPath = "data/sample_lda_libsvm_data.txt"
  val dataset = spark.read.format("libsvm").load(inputPath)

  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaled")
    .setWithMean(true)
    .setWithStd(true)

  override val sparkTransformer = scaler.fit(dataset)

  val result = sparkTransformer.transform(dataset)
  override val input = withColumnAsArray(result, scaler.getInputCol).toJSON.collect()
  override val expectedOutput = withColumnAsArray(result, scaler.getOutputCol).toJSON.collect()

  test("StandardScaler w/o Mean and Std") {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled")
      .setWithMean(false)
      .setWithStd(false)
    val sparkTransformer = scaler.fit(dataset)
    val result = sparkTransformer.transform(dataset)
    val expectedOutput = withColumnAsArray(result, scaler.getOutputCol).toJSON.collect()
    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("StandardScaler w/o Mean") {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled")
      .setWithMean(false)
      .setWithStd(true)
    val sparkTransformer = scaler.fit(dataset)
    val result = sparkTransformer.transform(dataset)
    val expectedOutput = withColumnAsArray(result, scaler.getOutputCol).toJSON.collect()
    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("StandardScaler w/o Std") {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled")
      .setWithMean(true)
      .setWithStd(false)
    val sparkTransformer = scaler.fit(dataset)
    val result = sparkTransformer.transform(dataset)
    val expectedOutput = withColumnAsArray(result, scaler.getOutputCol).toJSON.collect()
    parityTest(sparkTransformer, input, expectedOutput)
  }

}

