package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.spark.ml.PFATransformer
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.RegexTokenizer


// TODO missing token count filter and gaps vs tokens
class PFARegexTokenizer(override val sparkTransformer: RegexTokenizer) extends PFATransformer {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  private val pattern = sparkTransformer.getPattern
  private val gaps = sparkTransformer.getGaps
  private val minTokenLength = sparkTransformer.getMinTokenLength
  private val toLowerCase = sparkTransformer.getToLowercase

  override def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(inputCol).`type`().stringType().noDefault()
      .endRecord()
  }

  override def outputSchema = {
    SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(outputCol).`type`().array().items().stringType().noDefault()
      .endRecord()
  }

  override def action: PFAExpression = {
    val a = if (toLowerCase) {
      re.split(s.lower(inputExpr), pattern)
    } else {
      re.split(inputExpr, pattern)
    }
    NewRecord(outputSchema, Map(outputCol -> a))
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
