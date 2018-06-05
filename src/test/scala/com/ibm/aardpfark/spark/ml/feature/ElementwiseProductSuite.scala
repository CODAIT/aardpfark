package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{ScalerResult, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors

class ElementwiseProductSuite extends SparkFeaturePFASuiteBase[ScalerResult] {

  val data = spark.createDataFrame(Seq(
    ("a", Vectors.dense(1.0, 2.0, 3.0)),
    ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

  val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
  override val sparkTransformer = new ElementwiseProduct()
    .setScalingVec(transformingVector)
    .setInputCol("vector")
    .setOutputCol("scaled")

  val result = sparkTransformer.transform(data)
  override val input = withColumnAsArray(result, sparkTransformer.getInputCol).toJSON.collect()
  override val expectedOutput = withColumnAsArray(result, sparkTransformer.getOutputCol).toJSON.collect()
}
