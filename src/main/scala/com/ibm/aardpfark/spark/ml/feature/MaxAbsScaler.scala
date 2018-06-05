package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.MaxAbsScalerModel

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.feature")
case class MaxAbsScalerModelData(maxAbs: Array[Double]) extends WithSchema {
  def schema = AvroSchema[this.type]
}

class PFAMaxAbsScalerModel(override val sparkTransformer: MaxAbsScalerModel) extends PFAModel[MaxAbsScalerModelData] {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  // cell data
  private val scalerData = MaxAbsScalerModelData(sparkTransformer.maxAbs.toArray)
  override def cell = Cell(scalerData)
  // references to cell variables
  private val maxAbs = modelCell.ref("maxAbs")

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

  val ifFn = If (core.gt(StringExpr("s"), 0.0)) Then core.div("i", "s") Else StringExpr("i")
  val divDoubleFn = NamedFunctionDef(
    "divDouble",
    FunctionDef[Double, Double](Seq("i", "s"), Seq(ifFn))
  )

  override def action: PFAExpression = {
    NewRecord(outputSchema, Map(outputCol -> a.zipmap(inputExpr, maxAbs, divDoubleFn.ref)))
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withFunction(divDoubleFn)
      .withAction(action)
      .pfa
  }
}
