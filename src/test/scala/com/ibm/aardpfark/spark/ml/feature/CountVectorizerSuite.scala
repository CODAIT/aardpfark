package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.CountVectorizer

class CountVectorizerSuite extends SparkFeaturePFASuiteBase[CountVectorizerResult] {

  val df = spark.createDataFrame(Seq(
    (0, Array("a", "b", "c", "d", "e", "f")),
    (1, Array("a", "b", "b", "c", "a"))
  )).toDF("id", "words")

  val cv = new CountVectorizer()
    .setInputCol("words")
    .setOutputCol("features")

  override val sparkTransformer = cv.fit(df)

  val result = sparkTransformer.transform(df)
  override val input = result.select(cv.getInputCol).toJSON.collect()
  override val expectedOutput = withColumnAsArray(result, cv.getOutputCol).toJSON.collect()

  // Additional test for MinTF
  test("CountVectorizer with MinTF = 0.3") {
    val cv = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(0.3)

    val sparkTransformer = cv.fit(df)

    val result = sparkTransformer.transform(df)
    val input = result.select(cv.getInputCol).toJSON.collect()
    val expectedOutput = withColumnAsArray(result, cv.getOutputCol).toJSON.collect()

    parityTest(sparkTransformer, input, expectedOutput)
  }

  test("CountVectorizer with MinTF = 2.0") {
    val cv = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(2.0)

    val sparkTransformer = cv.fit(df)

    val result = sparkTransformer.transform(df)
    val input = result.select(cv.getInputCol).toJSON.collect()
    val expectedOutput = withColumnAsArray(result, cv.getOutputCol).toJSON.collect()

    parityTest(sparkTransformer, input, expectedOutput)
  }

  // Additional test for binary
  test("CountVectorizer with binary") {
    val cv = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setBinary(true)

    val sparkTransformer = cv.fit(df)

    val result = sparkTransformer.transform(df)
    val input = result.select(cv.getInputCol).toJSON.collect()
    val expectedOutput = withColumnAsArray(result, cv.getOutputCol).toJSON.collect()

    parityTest(sparkTransformer, input, expectedOutput)
  }

}

case class CountVectorizerResult(features: Seq[Double]) extends Result