package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.Bucketizer

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.feature")
case class BucketizerData(splits: Seq[Double]) extends WithSchema {
  override def schema = AvroSchema[this.type]
}

/**
 * Throws exception on out of range as in Spark
 * TODO: handle NaNs
 * @param sparkTransformer Spark Bucketizer model
 */
class PFABucketizer(override val sparkTransformer: Bucketizer) extends PFAModel[BucketizerData] {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  override def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(inputCol).`type`().doubleType().noDefault()
      .endRecord()
  }

  override def outputSchema = {
    SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(outputCol).`type`().doubleType().noDefault()
      .endRecord()
  }

  override protected def cell = Cell(BucketizerData(sparkTransformer.getSplits))
  private val doubleArraySchema = SchemaBuilder.array().items().doubleType()

  private val inBucketFn = NamedFunctionDef("inBucket", FunctionDef[Boolean](
    Seq(Param("x", doubleArraySchema), Param[Double]("y"), Param[Int]("op"))) {
    case Seq(x, y, op) =>
      val lower = Let("lower", Attr(x.ref, 0))
      val upper = Let("upper", Attr(x.ref, 1))
      val ifCond = If (core.eq(op.ref, 0)) Then {
        core.lt(y.ref, upper.ref)
      } Else {
        core.lte(y.ref, upper.ref)
      }
      val andCond = core.and(
        core.gte(y.ref, lower.ref),
        ifCond
      )
      Seq(
        lower,
        upper,
        andCond
      )
  }
  )

  override def action: PFAExpression = {
    val buckets = Let("buckets", a.slidingWindow(modelCell.ref("splits"), 2, 1))
    val lastIdx = Let("lastIdx", core.minus(a.len(buckets.ref), 1))
    val fills = Map[String, PFAExpression](
      "y" -> cast.double(inputExpr),
      "op" -> 0
    )
    val cond = If {
      inBucketFn(Attr(buckets.ref, lastIdx.ref), cast.double(inputExpr), 1)
    } Then {
      lastIdx.ref
    } Else {
      a.index(a.subseq(buckets.ref, 0, lastIdx.ref), inBucketFn.fill(fills))
    }
    val bucketIdx = Let("bucketIdx", cond)
    val returnCond = If (core.lt(bucketIdx.ref, 0)) Then {
      Error("Input value out of Bucketizer bucket ranges")
    } Else {
      bucketIdx.ref
    }
    Action(
      buckets,
      lastIdx,
      bucketIdx,
      NewRecord(outputSchema, Map(outputCol -> returnCond), true)
    )
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withFunction(inBucketFn)
      .withAction(action)
      .pfa
  }

}