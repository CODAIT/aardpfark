package com.ibm.aardpfark.spark.ml.feature

import org.apache.spark.ml.feature.VectorSlicer

class PFAVectorSlicer(override val sparkTransformer: VectorSlicer) extends PFAVectorSelector {
  override protected val inputCol: String = sparkTransformer.getInputCol
  override protected val outputCol: String = sparkTransformer.getOutputCol
  override protected val indices: Seq[Int] = sparkTransformer.getIndices
}
