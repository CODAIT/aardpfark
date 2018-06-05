package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.ElementwiseProduct

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.feature")
case class ElementwiseProductData(scalingVec: Seq[Double]) extends WithSchema {
  def schema = AvroSchema[this.type]
}

class PFAElementwiseProduct(override val sparkTransformer: ElementwiseProduct) extends PFAModel[ElementwiseProductData] {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  // cell data
  private val scalingVec = sparkTransformer.getScalingVec.toArray

  override def cell = Cell(ElementwiseProductData(scalingVec))
  // references to cell variables
  private val scalingVecRef = modelCell.ref("scalingVec")

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

  val scaleFn = NamedFunctionDef("doubleMult", FunctionDef[Double, Double](
    Seq("x", "y"),
    Seq(core.mult("x", "y"))
  ))

  override def action: PFAExpression = {
    val scale = a.zipmap(inputExpr, scalingVecRef, scaleFn.ref)
    NewRecord(outputSchema, Map(outputCol -> scale))
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withFunction(scaleFn)
      .withAction(action)
      .pfa
  }
}
