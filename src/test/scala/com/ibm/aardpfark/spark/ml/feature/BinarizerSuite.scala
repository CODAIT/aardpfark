package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.Binarizer

class BinarizerSuite extends SparkFeaturePFASuiteBase[BinarizerResult] {

  val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
  val dataFrame = spark.createDataFrame(data).toDF("id", "feature")
  val binarizer: Binarizer = new Binarizer()
    .setInputCol("feature")
    .setOutputCol("binarized_feature")
    .setThreshold(0.5)
  override val sparkTransformer = binarizer

  val result = binarizer.transform(dataFrame)

  // Need to specify type since a union is expected
  override val input = Array("{\"feature\":{\"double\":0.1}}", "{\"feature\":{\"double\":0.8}}", "{\"feature\":{\"double\":0.2}}")
  override val expectedOutput = result.select(binarizer.getOutputCol).toJSON.collect()
}

case class BinarizerResult(binarized_feature: Double) extends Result