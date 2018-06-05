package com.ibm.aardpfark.spark.ml.clustering

import com.ibm.aardpfark.pfa.{PredictorResult, SparkPredictorPFASuiteBase}

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class KMeansModelSuite extends SparkPredictorPFASuiteBase[PredictorResult] {

  implicit val enc = ExpressionEncoder[Vector]()

  val inputPath = "data/sample_kmeans_data.txt"
  val dataset = spark.read.format("libsvm").load(inputPath)

  val kmeans = new KMeans()
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
  override val sparkTransformer = kmeans.fit(dataset)

  val result = sparkTransformer.transform(dataset)
  override val input = withColumnAsArray(result, kmeans.getFeaturesCol).toJSON.collect()
  override val expectedOutput = result.select(kmeans.getPredictionCol).toJSON.collect()
}

