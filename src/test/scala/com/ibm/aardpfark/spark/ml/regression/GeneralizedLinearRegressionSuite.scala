package com.ibm.aardpfark.spark.ml.regression


import com.ibm.aardpfark.pfa.GLMResult

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.sql.{Column, Dataset}


class GeneralizedLinearRegressionSuite extends SparkRegressorPFASuiteBase[GLMResult] {
  import com.ibm.aardpfark.spark.ml.SparkMLTestUtils._
  import spark.implicits._

  import org.apache.spark.sql.functions._

  private val seed = 42

  private val nPoints = 100
  private val intercept = 2.5
  private val xMean = Array(2.9, 10.5)
  private val xVariance = Array(0.7, 1.2)
  private val noise = 0.01

  private def getDataset(coef: Array[Double], family: String, link: String) = {
    generateGeneralizedLinearRegressionInput(
      intercept, coef, xMean, xVariance, nPoints, seed, noise, family, link).toDF
  }

  private val dfGaussianIdentity = getDataset(Array(2.2, 0.6), "gaussian", "identity")
  private val dfGaussianLog = getDataset(Array(0.22, 0.06), "gaussian", "log")
  private val dfGaussianInverse = getDataset(Array(2.2, 0.6), "gaussian", "inverse")
  private val dfPoissonLog = getDataset(Array(0.22, 0.06), "poisson", "log")
  private val dfPoissonIdentity = getDataset(Array(2.2, 0.6), "poisson", "identity")
  private val dfPoissonSqrt = getDataset(Array(2.2, 0.6), "poisson", "sqrt")
  private val dfGammaInverse = getDataset(Array(2.2, 0.6), "gamma", "inverse")
  private val dfGammaIdentity = getDataset(Array(2.2, 0.6), "gamma", "identity")
  private val dfGammaLog = getDataset(Array(0.22, 0.06), "gamma", "log")

  val multiclassInputPath = "data/sample_multiclass_classification_data.txt"
  val multiclassDataset = spark.read.format("libsvm").load(multiclassInputPath)
  val binaryDataset = multiclassDataset
    .select(when(col("label") > 1, 1).otherwise(0).as("label"), col("features"))

  val dfTweedie = Seq(
    LabeledPoint(1.0, Vectors.dense(0.0, 5.0)),
    LabeledPoint(0.5, Vectors.dense(1.0, 2.0)),
    LabeledPoint(1.0, Vectors.dense(2.0, 1.0)),
    LabeledPoint(2.0, Vectors.dense(3.0, 3.0))
  ).toDF

  // default is family=gaussian / link=identity
  val glr = new GeneralizedLinearRegression()
    .setLinkPredictionCol("link")
    .setRegParam(1e-3)
  override val sparkTransformer = glr.fit(dfGaussianIdentity)
  val result = sparkTransformer.transform(dfGaussianIdentity)

  val columns = Seq(glr.getPredictionCol, glr.getLinkPredictionCol).map(col)

  override val input = withColumnAsArray(result, glr.getFeaturesCol).toJSON.collect()
  override val expectedOutput = result.select(columns: _*).toJSON.collect()

  private def testExplicitFamilyAndLink(
      glr: GeneralizedLinearRegression,
      family: String,
      link: Option[String] = None,
      data: Dataset[_],
      columns: Seq[Column]) = {
    val glrCopy = glr.copy(ParamMap.empty).setFamily(family)
    link.foreach(glrCopy.setLink)
    for (flag <- Seq(true, false)) {
      glrCopy.setFitIntercept(flag)
      val model = glrCopy.fit(data)
      val result = model.transform(data)
      val input = withColumnAsArray(result, glr.getFeaturesCol).toJSON.collect()
      val expectedOutput = result.select(columns: _*).toJSON.collect()

      parityTest(model, input, expectedOutput)
    }
  }

  test("default links for each family") {
    val familyAndDf = Seq(
      ("gaussian", dfGaussianIdentity),
      ("binomial", binaryDataset),
      ("poisson", dfPoissonLog),
      ("gamma", dfGammaInverse)
    )
    for ((family, df) <- familyAndDf ){
      testExplicitFamilyAndLink(glr, family, None, df, columns)
    }
  }

  test("linkPredictionCol not set") {
    val glr = new GeneralizedLinearRegression()
      .setRegParam(1e-3)

    val model = glr.fit(dfGaussianIdentity)
    val result = model.transform(dfGaussianIdentity)
    val input = withColumnAsArray(result, glr.getFeaturesCol).toJSON.collect()
    val expectedOutput = result.select(glr.getPredictionCol).toJSON.collect()
    parityTest(model, input, expectedOutput)

    //testExplicitFamilyAndLink(
      //glr, "gaussian", None, dfGaussianIdentity, Seq(glr.getPredictionCol).map(col))
  }

  test("Set family and link: Gaussian") {
    for (link <- Seq("identity", "log", "inverse")) {
      withClue(s"Failed for link=$link. ") {
        val df = link match {
          case "identity" =>
            dfGaussianIdentity
          case "log" =>
            dfGaussianLog
          case "inverse" =>
            dfGaussianInverse
        }
        testExplicitFamilyAndLink(glr, "gaussian", Some(link), df, columns)
      }
    }
  }

  test("Set family and link: Binomial family") {
    for (link <- Seq("logit", "probit", "cloglog")) {
      withClue(s"Failed for link=$link. ") {
        testExplicitFamilyAndLink(glr, "binomial", Some(link), binaryDataset, columns)
      }
    }
  }

  test("Set family and link: Poisson family") {
    for (link <- Seq("log", "identity", "sqrt")) {
      withClue(s"Failed for link=$link. ") {
        val df = link match {
          case "log" =>
            dfPoissonLog
          case "identity" =>
            dfPoissonIdentity
          case "sqrt" =>
            dfPoissonSqrt
        }
        testExplicitFamilyAndLink(glr, "poisson", Some(link), df, columns)
      }
    }
  }

  test("Set family and link: Gamma family") {
    for (link <- Seq("inverse", "identity", "log")) {
      withClue(s"Failed for link=$link. ") {
        val df = link match {
          case "inverse" =>
            dfGammaInverse
          case "identity" =>
            dfGammaIdentity
          case "log" =>
            dfGammaLog
        }
        testExplicitFamilyAndLink(glr, "gamma", Some(link), df, columns)
      }
    }
  }

  test("Set family, link power and variance power: Tweedie family") {
    val glrCopy = glr.copy(ParamMap.empty).setFamily("tweedie")
    // test linkPower not set => linkPower = 1 - variancePower
    for (variancePower <- Seq(0.0, 1.0, 1.6, 2.5)) {
      glrCopy.setVariancePower(variancePower)
      val model = glrCopy.fit(dfTweedie)
      val result = model.transform(dfTweedie)
      val input = withColumnAsArray(result, glr.getFeaturesCol).toJSON.collect()
      val expectedOutput = result.select(columns: _*).toJSON.collect()
      parityTest(model, input, expectedOutput)
    }
    // test linkPower set, variancePower is not used in prediction?
    for (linkPower <- Seq(0.0, 1.0, -1.0, 0.5, 2.0, 3.0);
         variancePower <- Seq(0.0, 1.0, 1.6, 2.5)) {
      glrCopy.setLinkPower(linkPower)
      glrCopy.setVariancePower(variancePower)
      val df = if (linkPower >= 2) {
        dfGammaIdentity
      } else {
        dfTweedie
      }
      val model = glrCopy.fit(df)
      val result = model.transform(df)
      val input = withColumnAsArray(result, glr.getFeaturesCol).toJSON.collect()
      val expectedOutput = result.select(columns: _*).toJSON.collect()
      parityTest(model, input, expectedOutput)
    }
  }

}
