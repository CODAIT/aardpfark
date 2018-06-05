package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.StandardScalerModel

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.feature")
case class StandardScalerModelData(mean: Seq[Double], std: Seq[Double]) extends WithSchema {
  def schema = AvroSchema[this.type]
}

class PFAStandardScalerModel(override val sparkTransformer: StandardScalerModel) extends PFAModel[StandardScalerModelData] {
  import com.ibm.aardpfark.pfa.dsl._
  import com.ibm.aardpfark.pfa.dsl.core._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  // references to cell variables
  private val meanRef = modelCell.ref("mean")
  private val stdRef = modelCell.ref("std")

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
    val scalerData = StandardScalerModelData(sparkTransformer.mean.toArray, sparkTransformer.std.toArray)
    Cell(scalerData)
  }

  def partFn(name: String, p: Seq[String], e: PFAExpression) = {
    NamedFunctionDef(name, FunctionDef[Double, Double](p, Seq(e)))
  }

  // function schema
  val (scaleFnDef, scaleFnRef) = if (sparkTransformer.getWithMean) {
    if (sparkTransformer.getWithStd) {
      val meanStdScale = partFn("meanStdScale", Seq("i", "m", "s"), div(minus("i", "m"), "s"))
      (Some(meanStdScale), a.zipmap(inputExpr, meanRef, stdRef, meanStdScale.ref))
    } else {
      val meanScale = partFn("meanScale", Seq("i", "m"), minus("i", "m"))
      (Some(meanScale), a.zipmap(inputExpr, meanRef, meanScale.ref))
    }
  } else {
    if (sparkTransformer.getWithStd) {
      val stdScale = partFn("stdScale", Seq("i", "s"), div("i", "s"))
      (Some(stdScale), a.zipmap(inputExpr, stdRef, stdScale.ref))
    } else {
      (None, inputExpr)
    }
  }

  override def action: PFAExpression = {
    NewRecord(outputSchema, Map(outputCol -> scaleFnRef))
  }

  override def pfa: PFADocument = {
    val builder = PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withAction(action)
    scaleFnDef.foreach(fnDef => builder.withFunction(fnDef))
    builder.pfa
  }
}
