package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.MinMaxScalerModel

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.feature")
case class MinMaxScalerModelData(
  originalMin: Seq[Double],
  originalRange: Seq[Double]) extends WithSchema {
  def schema = AvroSchema[this.type]
}

class PFAMinMaxScalerModel(override val sparkTransformer: MinMaxScalerModel) extends PFAModel[MinMaxScalerModelData] {
  import com.ibm.aardpfark.pfa.dsl._
  import com.ibm.aardpfark.pfa.dsl.core._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  // cell data
  private val originalMin = sparkTransformer.originalMin.toArray
  private val originalRange = sparkTransformer.originalMax.toArray.zip(originalMin).map { case (max, min) =>
    max - min
  }

  // references to cell variables
  private val originalMinRef = modelCell.ref("originalMin")
  private val originalRangeRef = modelCell.ref("originalRange")

  override def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(inputCol).`type`().array().items().doubleType().noDefault()
      .endRecord()
  }

  override def outputSchema = {
    SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(outputCol).`type`().array().items().doubleType().noDefault()
      .endRecord()
  }

  override def cell = {
    val scalerData = MinMaxScalerModelData(originalMin, originalRange)
    Cell(scalerData)
  }

  // local double literals
  private val scale = sparkTransformer.getMax - sparkTransformer.getMin
  private val min = sparkTransformer.getMin
  val cond = If (net(StringExpr("range"), 0.0)) Then div(minus("i", "min"), "range") Else 0.5
  val minMaxScaleFn = NamedFunctionDef("minMaxScale", FunctionDef[Double, Double](
    Seq("i", "min", "range"),
    Seq(plus(mult(cond, scale), min))))

  override def action: PFAExpression = {
    NewRecord(outputSchema,
      Map(outputCol -> a.zipmap(inputExpr, originalMinRef, originalRangeRef, minMaxScaleFn.ref)))
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withFunction(minMaxScaleFn)
      .withAction(action)
      .pfa
  }
}
