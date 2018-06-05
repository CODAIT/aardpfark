package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import com.opendatagroup.hadrian.errors.PFAUserException

import org.apache.spark.SparkException
import org.apache.spark.ml.feature.StringIndexer

class StringIndexerModelSuite extends SparkFeaturePFASuiteBase[StringIndexerResult] {
  import spark.implicits._

  val df = Seq(
    (0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")
  ).toDF("id", "category")

  val testHandleInvalidDF = Seq(
    (0, "a"), (1, "b"), (2, "c"), (3, "d"), (4, "e"), (5, "c")
  ).toDF("id", "category")

  val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")

  override val sparkTransformer = indexer.fit(df)

  val result = sparkTransformer.transform(df)
  val sparkOutput = result.select(indexer.getOutputCol).toDF()

  override val input = result.select(indexer.getInputCol).toJSON.collect()
  override val expectedOutput = sparkOutput.toJSON.collect()

  // Additional test for handleInvalid
  test("StringIndexer with handleInvalid=keep") {
    val sparkTransformer = indexer.setHandleInvalid("keep").fit(df)

    val result = sparkTransformer.transform(testHandleInvalidDF)
    val input = testHandleInvalidDF.select(indexer.getInputCol).toJSON.collect()
    val expectedOutput = result.select(indexer.getOutputCol).toJSON.collect()

    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("StringIndexer with handleInvalid=error") {
    val sparkTransformer = indexer.setHandleInvalid("error").fit(df)

    intercept[SparkException] {
      val result = sparkTransformer.transform(testHandleInvalidDF)
      result.foreach(_ => Unit)
    }

    intercept[PFAUserException] {
      val input = testHandleInvalidDF.select(indexer.getInputCol).toJSON.collect()
      // we transform on df here to avoid Spark throwing the error and to ensure we match
      // the sizes of expected input / output. The error should be thrown before the comparison
      // would fail
      val expectedOutput = sparkTransformer.transform(df).select(indexer.getOutputCol).toJSON.collect()
      parityTest(sparkTransformer, input, expectedOutput)
    }
  }
}

case class StringIndexerResult(categoryIndex: Double) extends Result