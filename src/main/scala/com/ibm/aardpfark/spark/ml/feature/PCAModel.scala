package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.{Schema, SchemaBuilder}

import org.apache.spark.ml.feature.PCAModel

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.feature")
case class PCAData(pc: Array[Array[Double]]) extends WithSchema {
  override def schema: Schema = AvroSchema[this.type]
}

class PFAPCAModel(override val sparkTransformer: PCAModel) extends PFAModel[PCAData] {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

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

  override protected def cell = {
    val pc = sparkTransformer.pc.transpose.rowIter.map(v => v.toArray).toArray
    Cell(PCAData(pc))
  }

  override def action: PFAExpression = {
    val dot = la.dot(modelCell.ref("pc"), inputExpr)
    NewRecord(outputSchema, Map(outputCol -> dot))
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
