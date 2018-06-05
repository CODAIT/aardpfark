package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAClassificationModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.{Schema, SchemaBuilder}

import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrix}

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.classification")
case class NaiveBayesModelData(
  pi: Seq[Double],
  theta: Seq[Seq[Double]],
  negThetaSum: Seq[Double],
  thetaMinusNegTheta: Seq[Seq[Double]]) extends WithSchema {
  override def schema: Schema = AvroSchema[this.type]
}

object NaiveBayesModelData {

  def matrixToArrays(m: Matrix) = {
    val (rows, cols) = (m.numRows, m.numCols)
    Array.tabulate(rows, cols) { case (i, j) => m(i, j) }.toSeq.map(_.toSeq)
  }

  private def log1minusExp(d: Double) = math.log(1.0 - math.exp(d))

  def getBernMatrixAndVector(m: Matrix): (Matrix, DenseVector) = {
    val newValues = m.toDense.values.map(v => log1minusExp(v))
    val negTheta = new DenseMatrix(m.numRows, m.numCols, newValues, m.isTransposed)
    val ones = new DenseVector(Array.fill(m.numCols) {1.0})
    val negThetaSum = negTheta.multiply(ones)

    val subValues = m.toDense.values.map(v => v - log1minusExp(v))
    val thetaMinusNegTheta = new DenseMatrix(m.numRows, m.numCols, subValues, m.isTransposed)
    (thetaMinusNegTheta, negThetaSum)
  }
}

class PFANaiveBayesModel(override val sparkTransformer: NaiveBayesModel) extends PFAClassificationModel[NaiveBayesModelData] {
  import com.ibm.aardpfark.pfa.dsl._

  private val probabilityCol = sparkTransformer.getProbabilityCol

  override def outputSchema = SchemaBuilder.record(withUid(outputBaseName)).fields()
    .name(rawPredictionCol).`type`().array().items().doubleType().noDefault()
    .name(predictionCol).`type`.doubleType().noDefault()
    .name(probabilityCol).`type`().array().items().doubleType().noDefault()
    .endRecord()


  override protected def cell: Cell[NaiveBayesModelData] = {
    val thetaArray = NaiveBayesModelData.matrixToArrays(sparkTransformer.theta)
    val data = if (modelType == "multinomial") {
      NaiveBayesModelData(sparkTransformer.pi.toArray, thetaArray, Seq(), Seq())
    } else {
      val (thetaMinusNegTheta, negThetaSum) = NaiveBayesModelData.getBernMatrixAndVector(
        sparkTransformer.theta)
      val thetaMinusArray = NaiveBayesModelData.matrixToArrays(thetaMinusNegTheta)
      NaiveBayesModelData(
        sparkTransformer.pi.toArray,
        thetaArray,
        negThetaSum.toArray,
        thetaMinusArray)
    }
    Cell(data)
  }

  private val modelType = sparkTransformer.getModelType // bernoulli or multinomial

  private val pi = modelCell.ref("pi")
  private val theta = modelCell.ref("theta")
  private val thetaMinusNegTheta = modelCell.ref("thetaMinusNegTheta")
  private val negThetaSum = modelCell.ref("negThetaSum")

  private def multinomialCalculation = {
    la.add(pi, la.dot(theta, inputExpr))
  }

  private def bernoulliCalculation = {
    val prob = la.dot(thetaMinusNegTheta, inputExpr)
    la.add(negThetaSum, la.add(pi, prob))
  }

  private def rawPredFn = if (modelType == "multinomial") {
    multinomialCalculation
  } else {
    bernoulliCalculation
  }
  private val rawPred = Let("rawPred", rawPredFn)
  private val prob = Let("prob", m.link.softmax(rawPred.ref))
  private val pred = Let("pred", a.argmax(prob.ref))

  override def action: PFAExpression = {
    Action(
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
