package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.ml.feature.StopWordsRemover

@AvroNamespace("com.ibm.aardpfark.exec.spark.spark.ml.feature")
case class StopWords(words: Seq[String]) extends WithSchema {
  def schema = AvroSchema[this.type ]
}

class PFAStopWordsRemover(override val sparkTransformer: StopWordsRemover) extends PFAModel[StopWords] {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  private val stopWords = sparkTransformer.getStopWords
  private val caseSensitive = sparkTransformer.getCaseSensitive

  private def filterFn = FunctionDef[String, Boolean]("word") { w =>
    Seq(core.not(a.contains(wordsRef, if (caseSensitive) w else s.lower(w))))
  }

  override def inputSchema: Schema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(inputCol).`type`().array().items().stringType().noDefault()
      .endRecord()
  }

  override def outputSchema: Schema =  {
    SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(outputCol).`type`().array().items().stringType().noDefault()
      .endRecord()
  }

  override protected def cell = {
    Cell(StopWords(stopWords))
  }

  private val wordsRef = modelCell.ref("words")

  override def action: PFAExpression = {
    NewRecord(outputSchema, Map(outputCol -> a.filter(inputExpr, filterFn)))
  }

  override def pfa: PFADocument =
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withAction(action)
      .pfa
}
