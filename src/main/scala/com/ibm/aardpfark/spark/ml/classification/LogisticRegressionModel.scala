package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.document.{PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.spark.ml.PFALinearPredictionModel
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.classification.LogisticRegressionModel


class PFALogisticRegressionModel(override val sparkTransformer: LogisticRegressionModel)
  extends PFALinearPredictionModel {

  private val rawPredictionCol = sparkTransformer.getRawPredictionCol
  private val probabilityCol = sparkTransformer.getProbabilityCol
  private val isBinary = sparkTransformer.numClasses == 2

  override def outputSchema = SchemaBuilder.record(withUid(outputBaseName)).fields()
    .name(rawPredictionCol).`type`().array().items().doubleType().noDefault()
    .name(predictionCol).`type`.doubleType().noDefault()
    .name(probabilityCol).`type`().array().items().doubleType().noDefault()
    .endRecord()

  private val safeDoubleDiv = NamedFunctionDef("safeDoubleDiv", FunctionDef[Double, Double]("x", "y") {
    case Seq(x, y) =>
      val result = Let("result", core.div(x, y))
      val cond = If (impute.isnan(result.ref)) Then {
        core.addinv(core.pow(10.0, 320.0))
      } Else {
        result.ref
      }
      Seq(
        result,
        cond
      )
  })

  private val rawPredFn = if (isBinary) {
    NewArray[Double](Seq(core.addinv(margin.ref), margin.ref))
  } else {
    margin.ref
  }
  private val probFn = if (isBinary) {
    m.link.logit(rawPredFn)
  } else {
    m.link.softmax(rawPredFn)
  }

  private val rawPred = Let("rawPred", rawPredFn)
  private val prob = Let("prob", probFn)

  private val predFn = if (isBinary) {
    val threshold = sparkTransformer.getThreshold
    val probAttr = Attr(prob.ref, 1)
    If (core.lte(probAttr, threshold)) Then 0.0 Else 1.0
  } else {
    val scaled = if (sparkTransformer.isDefined(sparkTransformer.thresholds)) {
      val thresholds = NewArray[Double](sparkTransformer.getThresholds.map(DoubleLiteral))
      a.zipmap(prob.ref, thresholds, safeDoubleDiv.ref)
    } else {
      prob.ref
    }
    a.argmax(scaled)
  }

  private val pred = Let("pred", predFn)

  override def action = {
    Action(
      margin,
      rawPred,
      prob,
      pred,
      NewRecord(outputSchema, Map(
        probabilityCol -> prob.ref,
        rawPredictionCol -> rawPred.ref,
        predictionCol -> pred.ref)
      )
    )
  }

  override def pfa: PFADocument = {
    val bldr = PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withAction(action)
    if (!isBinary) {
      bldr.withFunction(safeDoubleDiv)
    }
    bldr.pfa
  }

}
