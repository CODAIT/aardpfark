package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.{Result, SparkFeaturePFASuiteBase}
import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer}

class BucketizerSuite extends SparkFeaturePFASuiteBase[BucketizerResult] {

  val splits = Array(-0.5, 0.0, 0.5, Double.PositiveInfinity)
  val data = Array(-0.5, -0.3, 0.0, 0.2, 999.9)
  val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

  override val sparkTransformer = new Bucketizer()
    .setInputCol("features")
    .setOutputCol("bucketedFeatures")
    .setSplits(splits)

  val result = sparkTransformer.transform(df)
  override val input = result.select(sparkTransformer.getInputCol).toJSON.collect()
  override val expectedOutput = result.select(sparkTransformer.getOutputCol).toJSON.collect()

  // Additional test for QuantileDiscretizer
  test("Bucketizer result from QuantileDiscretizer") {

    val df = spark.range(10, 1000, 3).toDF("input")

    val qd = new QuantileDiscretizer()
      .setInputCol("input")
      .setOutputCol("bucketedFeatures")
      .setNumBuckets(10)

    val bucketizer = qd.fit(df)
    val expectedOutput = bucketizer.transform(df)
    parityTest(bucketizer, df.select(bucketizer.getInputCol).toJSON.collect(), expectedOutput.toJSON.collect())
  }

}

case class BucketizerResult(bucketedFeatures: Double) extends Result