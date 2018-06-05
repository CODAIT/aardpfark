package com.ibm.aardpfark.spark.ml.regression

import com.ibm.aardpfark.pfa.{Result, SparkPredictorPFASuiteBase}

abstract class SparkRegressorPFASuiteBase[A <: Result](implicit m: Manifest[A]) extends SparkPredictorPFASuiteBase[A] {

  protected val inputPath = "data/sample_linear_regression_data.txt"

}
