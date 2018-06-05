package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}

import org.apache.spark.ml.feature.NGram


class NGramSuite extends SparkFeaturePFASuiteBase[NGramResult] {

  val data = spark.createDataFrame(Seq(
    (0, Array("Hi", "I", "heard", "about", "Spark")),
    (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
    (2, Array("Logistic", "regression", "models", "are", "neat")),
    (3, Array("Logistic", "regression")),
    (4, Array("Logistic")),
    (5, Array[String]())
  )).toDF("id", "words")

  override val sparkTransformer = new NGram()
    .setInputCol("words")
    .setOutputCol("ngrams")

  val result = sparkTransformer.transform(data)
  override val input = result.select(sparkTransformer.getInputCol).toJSON.collect()
  override val expectedOutput = result.select(sparkTransformer.getOutputCol).toJSON.collect()

  test("ngrams = 1") {
    val transformer = new NGram()
      .setInputCol("words")
      .setOutputCol("ngrams")
      .setN(1)

    val result = transformer.transform(data)
    val input = result.select(sparkTransformer.getInputCol).toJSON.collect()
    val expectedOutput = result.select(sparkTransformer.getOutputCol).toJSON.collect()
    parityTest(transformer, input, expectedOutput)
  }

  test("ngrams = 3") {
    val transformer = new NGram()
      .setInputCol("words")
      .setOutputCol("ngrams")
      .setN(3)

    val result = transformer.transform(data)
    val input = result.select(sparkTransformer.getInputCol).toJSON.collect()
    val expectedOutput = result.select(sparkTransformer.getOutputCol).toJSON.collect()
    parityTest(transformer, input, expectedOutput)
  }
}

case class NGramResult(ngrams: Seq[String]) extends Result