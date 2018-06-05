package com.ibm.aardpfark.spark.ml

import scala.util.Random

import breeze.linalg.DenseVector

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.random.{GammaGenerator, PoissonGenerator, StandardNormalGenerator}

object SparkMLTestUtils {

  def generateGeneralizedLinearRegressionInput(
    intercept: Double,
    coefficients: Array[Double],
    xMean: Array[Double],
    xVariance: Array[Double],
    nPoints: Int,
    seed: Int,
    noiseLevel: Double,
    family: String,
    link: String): Seq[LabeledPoint] = {

    val rnd = new Random(seed)
    def rndElement(i: Int) = {
      (rnd.nextDouble() - 0.5) * math.sqrt(12.0 * xVariance(i)) + xMean(i)
    }
    val (generator, mean) = family match {
      case "gaussian" => (new StandardNormalGenerator, 0.0)
      case "poisson" => (new PoissonGenerator(1.0), 1.0)
      case "gamma" => (new GammaGenerator(1.0, 1.0), 1.0)
    }
    generator.setSeed(seed)

    (0 until nPoints).map { _ =>
      val x = DenseVector(coefficients.indices.map(rndElement).toArray)
      val w = DenseVector(coefficients)
      val eta = w.dot(x) + intercept
      val mu = link match {
        case "identity" => eta
        case "log" => math.exp(eta)
        case "sqrt" => math.pow(eta, 2.0)
        case "inverse" => 1.0 / eta
      }
      val label = mu + noiseLevel * (generator.nextValue() - mean)
      // Return LabeledPoints with DenseVector
      LabeledPoint(label, Vectors.dense(x.data))
    }
  }

}
