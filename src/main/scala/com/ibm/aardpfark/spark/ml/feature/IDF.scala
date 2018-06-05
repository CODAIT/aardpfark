package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAModel
import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.apache.avro.{Schema, SchemaBuilder}

import org.apache.spark.ml.feature.IDFModel

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.feature")
case class IDFData(idf: Seq[Double]) extends WithSchema {
  override def schema: Schema = AvroSchema[this.type]
}

/*
  TODO - Add support for sparse vectors
  case class IDFData(idf: Map[String, Double]) extends WithSchema {
    override def schema: Schema = AvroSchema[this.type]
}*/

class PFAIDFModel(override val sparkTransformer: IDFModel) extends PFAModel[IDFData] {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

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

  override protected def cell = {

    // idf vector cell storage
    val denseIdf = sparkTransformer.idf.toDense.toArray.toSeq
    Cell(IDFData(denseIdf))

  /*
    TODO - Add support for sparse vectors
    val sparseIdf = collection.mutable.HashMap[String, Double]()
    sparkTransformer.idf.toSparse.foreachActive { case (i, v) => sparseIdf.put(i.toString, v) }
    Cell(IDFData(sparseIdf.toMap))
  */

  }
  private val idfRef = modelCell.ref("idf")

  val multFn = NamedFunctionDef("mult", FunctionDef[Double, Double]("x", "y") { case Seq(x, y) =>
     core.mult(x, y)
  })

  /*
    TODO - Add support for sparse vectors
    private val cond = If (map.containsKey(idfRef, StringExpr("k"))) Then {
    core.mult(Attr(idfRef, StringExpr("k")), StringExpr("v"))
  } Else {
    StringExpr("v")
  }
  private val idfScaleFn = NamedFunctionDef("idfScale", FunctionDef[Double](
    Seq(Param[Double]("v")), Seq(cond))
  )*/


  override def action: PFAExpression = {
    NewRecord(outputSchema, Map(outputCol -> a.zipmap(inputExpr, idfRef, multFn.ref)))
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withFunction(multFn)
      .withAction(action)
      .pfa
  }

}