package com.ibm.aardpfark.pfa

import com.ibm.aardpfark.spark.ml.KMeansPipelineResult
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import org.json4s.native.JsonMethods.parse

abstract class SparkPredictorPFASuiteBase[A <: Result](implicit m: Manifest[A])
  extends SparkFeaturePFASuiteBase[A] {

  override protected def testInputVsExpected(
    engine: PFAEngine[AnyRef, AnyRef],
    input: Array[String],
    expectedOutput: Array[String]) = {
    import ApproxEquality._
    import org.scalactic.Tolerance._
    val tol = 0.0001
    input.zip(expectedOutput).foreach { case (in, out) =>
      val pfaResult = engine.action(engine.jsonInput(in))
      val actual = parse(engine.jsonOutput(pfaResult)).extract[A]
      val expected = parse(out).extract[A]
      (actual, expected) match {
        case (a: PredictorResult, e: PredictorResult) =>
          assert(a.prediction === e.prediction +- tol)
        case (a: GLMResult, e: GLMResult) =>
          assert(a.prediction === e.prediction +- tol)
          assert(a.link === e.link +- tol)
        case (a: ClassifierResult, e: ClassifierResult) =>
          assert(a.prediction === e.prediction)
          assert(a.rawPrediction === e.rawPrediction)
        case (a: ProbClassifierResult, e: ProbClassifierResult) =>
          assert(a.prediction === e.prediction)
          assert(a.rawPrediction === e.rawPrediction)
          assert(a.probability === e.probability)
        case (a: KMeansPipelineResult, e: KMeansPipelineResult) =>
          assert(a.prediction === e.prediction)
          assert(a.ewp === e.ewp)
          assert(a.pca === e.pca)
          assert(a.s1 === e.s1)
          assert(a.s2 === e.s2)
          assert(a.kmeans === e.kmeans)
        case (_, _) =>
      }
    }
  }

}

case class PredictorResult(prediction: Double) extends Result

case class GLMResult(prediction: Double, link: Double = 0.0 /* default for when no linkPredictionCol is set*/) extends Result

case class ClassifierResult(prediction: Double, rawPrediction: Seq[Double]) extends Result

case class ProbClassifierResult(
  prediction: Double,
  rawPrediction: Seq[Double],
  probability: Seq[Double]) extends Result