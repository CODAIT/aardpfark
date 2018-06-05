package com.ibm.aardpfark.spark.ml

import com.ibm.aardpfark.pfa.document._
import com.ibm.aardpfark.pfa.dsl.StringExpr
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.linear.{DenseLinearModelData, LinearModelData, SparkLinearModel, SparseLinearModelData}
import org.apache.avro.{Schema, SchemaBuilder}

import org.apache.spark.ml.classification.{ClassificationModel, LinearSVCModel, LogisticRegressionModel}
import org.apache.spark.ml.regression.{GeneralizedLinearRegressionModel, LinearRegressionModel}
import org.apache.spark.ml.{PredictionModel, Transformer}


trait PFATransformer extends HasAction with ToPFA {

  protected val sparkTransformer: Transformer

  protected val inputBaseName: String = "Input"
  protected def inputSchema: Schema
  protected val outputBaseName: String = "Output"
  protected def outputSchema: Schema

  protected def withUid(name: String) = {
    s"${name}_${sparkTransformer.uid}"
  }

  protected def getMetadata: Map[String, String] = {
    sparkTransformer.extractParamMap().toSeq.map(pair => (pair.param.toString(), pair.value.toString)).toMap
  }
}

trait PFAModel[T <: WithSchema] extends PFATransformer with HasModelCell {
  protected def cell: Cell[T]
  override protected lazy val modelCell = NamedCell(sparkTransformer.uid, cell)
}

trait PFAPredictionModel[T <: WithSchema] extends PFAModel[T] {

  override protected val sparkTransformer: PredictionModel[_, _]
  protected val featuresCol: String = sparkTransformer.getFeaturesCol
  protected val predictionCol: String = sparkTransformer.getPredictionCol
  protected val inputExpr = StringExpr(s"input.${featuresCol}")

  override protected def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(featuresCol).`type`().array().items().doubleType().noDefault()
      .endRecord()
  }

  override protected def outputSchema = {
    SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(predictionCol).`type`().doubleType().noDefault()
      .endRecord()
  }
}

trait PFAClassificationModel[T <: WithSchema] extends PFAPredictionModel[T] {
  override protected val sparkTransformer: ClassificationModel[_, _]
  protected val rawPredictionCol: String = sparkTransformer.getRawPredictionCol
}

trait PFALinearPredictionModel extends PFAPredictionModel[LinearModelData] {
  import com.ibm.aardpfark.pfa.dsl._

  private val denseInputSchema = SchemaBuilder.record(withUid(inputBaseName)).fields()
    .name(featuresCol).`type`().array().items().doubleType().noDefault()
    .endRecord()

  private val sparseInputSchema = SchemaBuilder.record(withUid(inputBaseName)).fields()
    .name(featuresCol).`type`().map().values().doubleType().noDefault()
    .endRecord()

  protected val margin = Let("margin", model.reg.linear(s"input.${featuresCol}", modelCell.ref))

  override protected def cell: Cell[LinearModelData] = {
    val lm = sparkTransformer match {
      case m: LinearRegressionModel =>
        SparkLinearModel(m.intercept, m.coefficients)
      case m: LogisticRegressionModel =>
        if (m.numClasses == 2) {
          SparkLinearModel(m.intercept, m.coefficients)
        } else {
          SparkLinearModel(m.interceptVector, m.coefficientMatrix)
        }
      case m: LinearSVCModel =>
        SparkLinearModel(m.intercept, m.coefficients)
      case m: GeneralizedLinearRegressionModel =>
        SparkLinearModel(m.intercept, m.coefficients)
    }
    Cell(lm)
  }

  override protected def inputSchema = {
    denseInputSchema
    /*
    TODO: support for generic vectors
    cell.init match {
      case d: DenseLinearModelData =>
        denseInputSchema
      case s: SparseLinearModelData =>
        sparseInputSchema
    }
    */
  }

  override protected def action = {
    Action(
      margin,
      NewRecord(outputSchema, Map(predictionCol -> margin.ref)))
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
