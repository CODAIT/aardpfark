package com.ibm.aardpfark.spark.ml.clustering

import com.ibm.aardpfark.pfa.dsl.StringExpr
import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.{Schema, SchemaBuilder}

import org.apache.spark.ml.clustering.KMeansModel

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.clustering")
case class Cluster(id: Int, center: Seq[Double])

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.clustering")
case class KMeansModelData(clusters: Seq[Cluster]) extends WithSchema {
  override def schema: Schema = AvroSchema[this.type]
}

class PFAKMeansModel(override val sparkTransformer: KMeansModel) extends PFAModel[KMeansModelData] {

  private val inputCol = sparkTransformer.getFeaturesCol
  private val outputCol = sparkTransformer.getPredictionCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  override def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(inputCol).`type`().array().items().doubleType().noDefault()
      .endRecord()
  }

  override def outputSchema = SchemaBuilder.record(withUid(outputBaseName)).fields()
    .name(outputCol).`type`().intType().noDefault()
    .endRecord()

  override def cell =  {
    val clusters = sparkTransformer.clusterCenters.zipWithIndex.map { case (v, i) =>
      Cluster(i, v.toArray)
    }
    Cell(KMeansModelData(clusters))
  }

  override def action: PFAExpression = {
    val closest = model.cluster.closest(inputExpr, modelCell.ref("clusters"))
    NewRecord(outputSchema, Map(outputCol -> Attr(closest, "id")))
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
