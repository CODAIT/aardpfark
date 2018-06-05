package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors


class VectorAssemblerSuite extends SparkFeaturePFASuiteBase[VectorAssemblerResult] {

  import spark.implicits._
  val data = Seq((0, 18, 1.0, 3.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
  val df = spark.createDataset(data).toDF("id", "hour", "mobile", "region", "userFeatures", "clicked")

  override val sparkTransformer = new VectorAssembler()
    .setInputCols(Array("hour", "mobile", "region", "userFeatures", "clicked"))
    .setOutputCol("features")

  val result = sparkTransformer.transform(df)
  val columnNames = sparkTransformer.getInputCols.toSeq
  override val input = Array(
    """{"hour":{"double":18},
      |"mobile":{"double":1.0},
      |"region":{"double":3.0},
      |"userFeatures":{"array":[0.0,10.0,0.5]},
      |"clicked":{"double":1.0}}""".stripMargin)

  override val expectedOutput = withColumnAsArray(result, sparkTransformer.getOutputCol).toJSON.collect()
}

case class VectorAssemblerResult(features: Seq[Double]) extends Result
