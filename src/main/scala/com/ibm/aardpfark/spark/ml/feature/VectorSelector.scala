package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.SchemaBuilder

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.feature")
case class VectorSelectorData(indices: Seq[Int]) extends WithSchema {
  override def schema = AvroSchema[this.type]
}

abstract class PFAVectorSelector extends PFAModel[VectorSelectorData] {
  import com.ibm.aardpfark.pfa.dsl._

  protected val inputCol: String
  protected val outputCol: String
  protected lazy val inputExpr = StringExpr(s"input.${inputCol}")

  protected val indices: Seq[Int]

  override protected def cell = Cell(VectorSelectorData(indices))

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

  private def filterFn = FunctionDef[Boolean](
    Seq(Param[Int]("idx"), Param[Double]("x")),
    Seq(a.contains(modelCell.ref("indices"), "idx"))
  )
  override def action: PFAExpression = {
    NewRecord(outputSchema, Map(outputCol -> a.filterWithIndex(inputExpr, filterFn)))
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
