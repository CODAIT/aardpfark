package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.PredictorResult
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

class MLPClassifierSuite extends SparkClassifierPFASuiteBase[PredictorResult] {

  val inputPath = "data/sample_multiclass_classification_data.txt"
  val data = spark.read.format("libsvm").load(inputPath)
  val layers = Array[Int](4, 5, 3)
  val trainer = new MultilayerPerceptronClassifier()
    .setLayers(layers)
    .setBlockSize(128)
    .setSeed(1234L)
    .setMaxIter(100)

  override val sparkTransformer = trainer.fit(data)

  val result = sparkTransformer.transform(data)
  override val input = withColumnAsArray(result, trainer.getFeaturesCol).toJSON.collect()
  override val expectedOutput = result.select(trainer.getPredictionCol).toJSON.collect()

}
