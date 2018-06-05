package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.ClassifierResult
import org.apache.spark.ml.classification.DecisionTreeClassifier

class DecisionTreeClassifierSuite extends SparkClassifierPFASuiteBase[ClassifierResult] {

  val inputPath = "data/sample_multiclass_classification_data.txt"
  val data = spark.read.format("libsvm").load(inputPath)
  val dt = new DecisionTreeClassifier()
    .setMaxDepth(5)
  override val sparkTransformer = dt.fit(data)

  val result = sparkTransformer.transform(data)
  override val input = withColumnAsArray(result, dt.getFeaturesCol).toJSON.collect()
  override val expectedOutput = result.select(dt.getPredictionCol)
    .toJSON.collect()

  //TODO Test with raw prediction and probability
}


