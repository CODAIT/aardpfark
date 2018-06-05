package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.PredictorResult
import org.apache.spark.ml.classification.RandomForestClassifier
class RandomForestClassificationSuite extends SparkClassifierPFASuiteBase[PredictorResult] {

  val inputPath = "data/sample_multiclass_classification_data.txt"
  val data = spark.read.format("libsvm").load(inputPath)

  val dt = new RandomForestClassifier()
    .setMaxDepth(3)
    .setNumTrees(3)
  override val sparkTransformer = dt.fit(data)

  val result = sparkTransformer.transform(data)
  override val input = withColumnAsArray(result, dt.getFeaturesCol).toJSON.collect()
  override val expectedOutput = result.select(dt.getPredictionCol).toJSON.collect()
}
