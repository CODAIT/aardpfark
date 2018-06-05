package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors

class VectorSlicerSuite extends SparkFeaturePFASuiteBase[VectorSlicerResult] {

  import spark.implicits._

  val data = Seq(
    (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
    (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
    (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
  )
  val df = spark.createDataset(data).toDF("id", "features", "label")

  override val sparkTransformer = new VectorSlicer()
    .setInputCol("features")
    .setIndices(Array(0, 1, 3))
    .setOutputCol("selectedFeatures")

  val result = sparkTransformer.transform(df)
  override val input = withColumnAsArray(result, sparkTransformer.getInputCol).toJSON.collect()
  override val expectedOutput = withColumnAsArray(result, sparkTransformer.getOutputCol).toJSON.collect()

}

case class VectorSlicerResult(selectedFeatures: Seq[Double]) extends Result