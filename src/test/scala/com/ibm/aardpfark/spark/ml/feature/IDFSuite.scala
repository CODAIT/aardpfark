package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector

class IDFSuite extends SparkFeaturePFASuiteBase[IDFResult] {

  val sentenceData = spark.createDataFrame(Seq(
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
  )).toDF("label", "sentence")

  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
  val wordsData = tokenizer.transform(sentenceData)

  val hashingTF = new HashingTF()
    .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
  val featurizedData = hashingTF.transform(wordsData)

  val toDense = udf({ v: Vector => v.toDense })
  val denseFeaturizedData = featurizedData.select(toDense(col("rawFeatures")))
      .toDF("rawFeatures")

  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

  override val sparkTransformer = idf.fit(denseFeaturizedData)

  val result = sparkTransformer.transform(denseFeaturizedData)
  override val input = withColumnAsArray(result, idf.getInputCol).toJSON.collect()
  override val expectedOutput = withColumnAsArray(result, idf.getOutputCol).toJSON.collect()

}

case class IDFResult(features: Seq[Double]) extends Result
