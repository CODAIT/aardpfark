package com.ibm.aardpfark.spark.ml.regression

import com.ibm.aardpfark.spark.ml.PFALinearPredictionModel

import org.apache.spark.ml.regression.LinearRegressionModel

class PFALinearRegressionModel(override val sparkTransformer: LinearRegressionModel)
  extends PFALinearPredictionModel