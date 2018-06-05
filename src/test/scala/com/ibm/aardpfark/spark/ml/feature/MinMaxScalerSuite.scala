package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{ScalerResult, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class MinMaxScalerSuite extends SparkFeaturePFASuiteBase[ScalerResult] {

  implicit val enc = ExpressionEncoder[Vector]()

  val inputPath = "data/sample_lda_libsvm_data.txt"
  val dataset = spark.read.format("libsvm").load(inputPath)

  val scaler = new MinMaxScaler()
    .setInputCol("features")
    .setOutputCol("scaled")

  override val sparkTransformer = scaler.fit(dataset)

  val result = sparkTransformer.transform(dataset)
  override val input = withColumnAsArray(result, scaler.getInputCol).toJSON.collect()
  override val expectedOutput = withColumnAsArray(result, scaler.getOutputCol).toJSON.collect()
}