package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.StringIndexerModel

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.feature")
case class Vocab(vocab: Map[String, Int]) extends WithSchema {
  def schema = AvroSchema[this.type]
}

class PFAStringIndexerModel(override val sparkTransformer: StringIndexerModel) extends PFAModel[Vocab] {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  private val handleInvalid = sparkTransformer.getHandleInvalid
  private val unknownLabel = sparkTransformer.labels.length

  override def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(inputCol).`type`().stringType().noDefault()
      .endRecord()
  }

  override def outputSchema = {
    val bldr = SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(outputCol).`type`()
    if (handleInvalid == "skip") {
      bldr.nullable().doubleType().noDefault().endRecord()
    } else {
      bldr.doubleType().noDefault().endRecord()
    }
  }

  override protected def cell = {
    val vocab = sparkTransformer.labels.zipWithIndex.toMap
    Cell(Vocab(vocab))
  }

  private val vocabRef = modelCell.ref("vocab")

  override def action: PFAExpression = {
    val inputAsStr = s.strip(cast.json(inputExpr), StringLiteral("\""))
    val mapper = If (map.containsKey(vocabRef, inputAsStr)) Then Attr(vocabRef, inputAsStr) Else {
      if (handleInvalid == "error") {
        Error("StringIndexer encountered unseen label")
      } else if (handleInvalid == "keep") {
        IntLiteral(unknownLabel)
      } else {
        NullLiteral
      }
    }
    NewRecord(outputSchema, Map(outputCol -> mapper), true)
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withAction(action)
      .pfa
  }

}
