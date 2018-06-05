package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.{Result, SparkPredictorPFASuiteBase}

abstract class SparkClassifierPFASuiteBase[A <: Result](implicit m: Manifest[A])
  extends SparkPredictorPFASuiteBase[A] {

}
