package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.RegexTokenizer


class RegexTokenizerSuite extends SparkFeaturePFASuiteBase[RegexTokenizerResult] {

  val data = spark.createDataFrame(Seq(
    (0L, "a b c D e Spark", 1.0),
    (1L, "b d", 0.0),
    (2L, "spark F g h", 1.0),
    (3L, "hadoop MapReduce", 0.0)
  )).toDF("id", "text", "label")

  override val sparkTransformer = new RegexTokenizer()
    .setPattern("[ \t]+")
    .setInputCol("text")
    .setOutputCol("words")

  val result = sparkTransformer.transform(data)
  override val input = result.select(sparkTransformer.getInputCol).toJSON.collect()
  override val expectedOutput = result.select(sparkTransformer.getOutputCol).toJSON.collect()

}

case class RegexTokenizerResult(words: Seq[String]) extends Result