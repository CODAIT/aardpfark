package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.spark.ml.PFATransformer
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.ml.feature.VectorAssembler
import org.json4s.DefaultFormats

class PFAVectorAssembler(override val sparkTransformer: VectorAssembler) extends PFATransformer {

  import com.ibm.aardpfark.pfa.dsl._
  implicit val formats = DefaultFormats

  private val inputCols = sparkTransformer.getInputCols
  private val outputCol = sparkTransformer.getOutputCol

  type DorSeqD = Either[Double, Seq[Double]]

  override protected def inputSchema: Schema = {
    val builder = SchemaBuilder.record(withUid(inputBaseName)).fields()
    for (inputCol <- inputCols) {
      builder.name(inputCol).`type`()
        .unionOf()
        .doubleType().and()
        .array().items().doubleType()
        .endUnion().noDefault()
    }
    builder.endRecord()
  }

  override protected def outputSchema: Schema = {
    SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(outputCol).`type`().array().items().doubleType().noDefault()
      .endRecord()
  }

  private val asDouble = As[Double]("x", x => NewArray[Double](x))
  private val asArray = As[Array[Double]]("x", x => x)

  private val castFn = NamedFunctionDef("castToArray",
    FunctionDef[DorSeqD, Seq[Double]]("x") { x =>
      Cast(x, asDouble, asArray)
    }
  )

  override protected def action: PFAExpression = {
    val cols = Let("cols", NewArray[DorSeqD](inputCols.map(c => StringExpr(s"input.$c"))))
    Action(
      cols,
      NewRecord(outputSchema, Map(outputCol -> a.flatten(a.map(cols.ref, castFn.ref))))
    )
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withAction(action)
      .withFunction(castFn)
      .pfa
  }
}
