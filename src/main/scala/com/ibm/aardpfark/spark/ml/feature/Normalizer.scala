package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.spark.ml.PFATransformer
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.Normalizer


class PFANormalizer(override val sparkTransformer: Normalizer) extends PFATransformer {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  private val p = sparkTransformer.getP

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

  private def absPow(p: Double) = FunctionDef[Double, Double](
    Seq("x"),
    Seq(core.pow(m.abs("x"), p))
  )

  private val sq = FunctionDef[Double, Double](
    Seq("x"),
    Seq(core.pow("x", 2.0))
  )

  private val absVal = FunctionDef[Double, Double](
    Seq("x"),
    Seq(m.abs("x"))
  )

  override def action: PFAExpression = {
    val fn = p match {
      case 1.0 =>
        a.sum(a.map(inputExpr, absVal))
      case 2.0 =>
        m.sqrt(a.sum(a.map(inputExpr, sq)))
      case Double.PositiveInfinity =>
        a.max(a.map(inputExpr, absVal))
      case _ =>
        core.pow(a.sum(a.map(inputExpr, absPow(p))), 1.0 / p)

    }
    val norm = Let("norm", fn)
    val invNorm = core.div(1.0, norm.ref)
    val scale = la.scale(inputExpr, invNorm)
    Action(
      norm,
      NewRecord(outputSchema, Map(outputCol -> scale))
    )
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      //.withFunction(pow(p))
      .withAction(action)
      .pfa
  }
}
