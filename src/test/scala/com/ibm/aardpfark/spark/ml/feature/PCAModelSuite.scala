package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class PCAModelSuite extends SparkFeaturePFASuiteBase[PCAModelResult] {

  implicit val enc = ExpressionEncoder[Vector]()

  val inputPath = "data/sample_lda_libsvm_data.txt"
  val dataset = spark.read.format("libsvm").load(inputPath)

  val pca = new PCA()
    .setInputCol("features")
    .setOutputCol("pcaFeatures")
    .setK(3)
  override val sparkTransformer = pca.fit(dataset)

  val result = sparkTransformer.transform(dataset)
  override val input = withColumnAsArray(result, pca.getInputCol).toJSON.collect()
  override val expectedOutput = withColumnAsArray(result, pca.getOutputCol).toJSON.collect()
}

case class PCAModelResult(pcaFeatures: Seq[Double]) extends Result