package com.ibm.aardpfark.spark.ml.classification

import scala.collection.mutable.ArrayBuffer

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAPredictionModel
import breeze.linalg.{DenseMatrix, DenseVector}
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.Schema

import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.classification")
case class Layer(weights: Array[Array[Double]], bias: Array[Double])
@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.classification")
case class Layers(layers: Seq[Layer]) extends WithSchema {
  override def schema: Schema = AvroSchema[this.type]
}

class PFAMultilayerPerceptronClassificationModel(
  override val sparkTransformer: MultilayerPerceptronClassificationModel)
  extends PFAPredictionModel[Layers] {

  private def getLayers = {
    val weights = sparkTransformer.weights.toArray
    val inputLayers = sparkTransformer.layers
    val layers = ArrayBuffer[Layer]()
    var offset = 0
    for (i <- 0 to inputLayers.size - 2) {
      val in = inputLayers(i)
      val out = inputLayers(i + 1)
      val wOffset = out * in
      val wData = weights.slice(offset, offset + wOffset)
      val bData = weights.slice(offset + wOffset, offset + wOffset + out)
      val w = Array.ofDim[Double](out, in)
      new DenseMatrix[Double](out, in, wData).foreachPair { case ((ii, jj), v) => w(ii)(jj) = v }
      val b = new DenseVector[Double](bData).toArray
      layers += Layer(w, b)
      offset += wOffset + out
    }
    layers.toArray
  }

  override protected def cell = Cell(Layers(getLayers))

  private val doubleSigmoid = NamedFunctionDef("doubleSigmoid", FunctionDef[Double, Double](
    "x", m.link.logit("x")
  ))

  override def action: PFAExpression = {
    val forward = model.neural.simpleLayers(inputExpr, modelCell.ref("layers"), doubleSigmoid.ref)
    val softmax = m.link.softmax(forward)
    NewRecord(outputSchema, Map(predictionCol -> a.argmax(softmax)))
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withFunction(doubleSigmoid)
      .withAction(action)
      .pfa
  }

}
