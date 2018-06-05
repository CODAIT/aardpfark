package com.ibm.aardpfark.spark.ml.feature

import org.apache.spark.ml.feature.ChiSqSelectorModel

class PFAChiSqSelectorModel(override val sparkTransformer: ChiSqSelectorModel) extends PFAVectorSelector {
  override protected val inputCol: String = sparkTransformer.getFeaturesCol
  override protected val outputCol: String = sparkTransformer.getOutputCol
  override protected val indices: Seq[Int] = sparkTransformer.selectedFeatures
}