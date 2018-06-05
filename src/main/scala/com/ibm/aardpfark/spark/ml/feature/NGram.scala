package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression.{PFAExpression, PartialFunctionRef}
import com.ibm.aardpfark.spark.ml.PFATransformer
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.NGram


class PFANGram(override val sparkTransformer: NGram) extends PFATransformer {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  private val n = sparkTransformer.getN

  override def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(inputCol).`type`().array().items().stringType().noDefault()
      .endRecord()
  }

  override def outputSchema = {
    SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(outputCol).`type`().array().items().stringType().noDefault()
      .endRecord()
  }

  override def action: PFAExpression = {
    // TODO - this partial fn reference is an ugly workaround for now - add support for builtin lib
    val partialFn = new PartialFunctionRef("s.join", Seq(("sep", " ")))
    val mapExpr = a.map(a.slidingWindow(inputExpr, n, 1), partialFn)
    NewRecord(outputSchema, Map(outputCol -> mapExpr))
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withAction(action)
      .pfa
  }
}
