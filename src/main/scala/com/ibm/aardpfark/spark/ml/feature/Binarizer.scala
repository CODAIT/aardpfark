package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.dsl.StringExpr
import com.ibm.aardpfark.pfa.document.{PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.spark.ml.PFATransformer
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.Binarizer

class PFABinarizer(override val sparkTransformer: Binarizer) extends PFATransformer {

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  override def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(inputCol).`type`().unionOf().array().items().doubleType()
      .and()
      .doubleType().endUnion()
      .noDefault()
      .endRecord()
  }

  override def outputSchema = {
    SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(outputCol).`type`().unionOf().array().items().doubleType()
      .and()
      .doubleType().endUnion()
      .noDefault()
      .endRecord()
  }

  private val th = sparkTransformer.getThreshold
  private val doubleBin = NamedFunctionDef("doubleBin", FunctionDef[Double, Double]("d",
    If (core.gt(StringExpr("d"), th)) Then 1.0 Else 0.0)
  )

  override def action: PFAExpression = {
    val asDouble = As[Double]("x", x => doubleBin.call(x))
    val asArray = As[Array[Double]]("x", x => a.map(x, doubleBin.ref))
    val cast = Cast(inputExpr,
      Seq(asDouble, asArray))
    NewRecord(outputSchema, Map(outputCol -> cast))
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withFunction(doubleBin)
      .withAction(action)
      .pfa
  }
}
