package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors

class ChiSqSelectorSuite extends SparkFeaturePFASuiteBase[ChiSqSelectorResult] {

  import spark.implicits._
  val data = Seq(
    (Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
    (Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
    (Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
  ).toDF("features", "label")

  val selector = new ChiSqSelector()
    .setNumTopFeatures(2)
    .setOutputCol("selectedFeatures")

  override val sparkTransformer = selector.fit(data)
  val result = sparkTransformer.transform(data)
  override val input = withColumnAsArray(result, selector.getFeaturesCol).toJSON.collect()
  override val expectedOutput = withColumnAsArray(result,selector.getOutputCol).toJSON.collect()

}

case class ChiSqSelectorResult(selectedFeatures: Seq[Double]) extends Result