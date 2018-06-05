package com.ibm.aardpfark.spark.ml.linear

import com.ibm.aardpfark.pfa.types.WithSchema
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}

abstract class LinearModelData extends WithSchema

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.linear")
case class DenseLinearModelData(const: Double, coeff: Seq[Double]) extends LinearModelData {
  def schema = AvroSchema[this.type]
}

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.linear")
case class MultiDenseLinearModelData(const: Seq[Double], coeff: Seq[Seq[Double]])
  extends LinearModelData {
  def schema = AvroSchema[this.type]
}

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.linear")
case class SparseLinearModelData(const: Double, coeff: Map[String, Double]) extends LinearModelData {
  def schema = AvroSchema[this.type]
}