package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.StopWordsRemover

class StopWordsRemoverSuite extends SparkFeaturePFASuiteBase[StopWordsResult]{

  val remover = new StopWordsRemover()
    .setInputCol("raw")
    .setOutputCol("filtered")

  val dataset = spark.createDataFrame(Seq(
    (0, Seq("I", "saw", "the", "red", "balloon")),
    (1, Seq("Mary", "had", "a", "little", "lamb")),
    (2, Seq("The", "the"))
  )).toDF("id", "raw")

  override val sparkTransformer = remover

  val result = sparkTransformer.transform(dataset)
  override val input = result.select(remover.getInputCol).toJSON.collect()
  override val expectedOutput = result.select(remover.getOutputCol).toJSON.collect()

  test("StopWordsRemover case sensitive") {
    val transformer = remover.setCaseSensitive(true)
    val result = transformer.transform(dataset)
    val input = result.select(remover.getInputCol).toJSON.collect()
    val expectedOutput = result.select(remover.getOutputCol).toJSON.collect()

    parityTest(transformer, input, expectedOutput)
  }
}

case class StopWordsResult(filtered: Seq[String]) extends Result