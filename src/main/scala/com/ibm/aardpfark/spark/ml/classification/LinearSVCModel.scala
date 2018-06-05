package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.spark.ml.PFALinearPredictionModel
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.classification.LinearSVCModel

class PFALinearSVCModel(override val sparkTransformer: LinearSVCModel)
  extends PFALinearPredictionModel {

  private val rawPredictionCol = sparkTransformer.getRawPredictionCol
  private val threshold = sparkTransformer.getThreshold

  override def outputSchema = SchemaBuilder.record(withUid(outputBaseName)).fields()
    .name(rawPredictionCol).`type`().array().items().doubleType().noDefault()
    .name(predictionCol).`type`.doubleType().noDefault()
    .endRecord()

  private val rawSchema = outputSchema.getField(rawPredictionCol).schema()
  private val rawPred = NewArray(rawSchema, Seq(core.addinv(margin.ref), margin.ref))
  private val pred = If (core.lte(margin.ref, threshold)) Then 0.0 Else 1.0
  override def action = {
    Action(
      margin,
      NewRecord(outputSchema, Map(
        predictionCol -> pred,
        rawPredictionCol -> rawPred)
      )
    )
  }
}
