package com.ibm.aardpfark.spark.ml

import com.ibm.aardpfark.avro.SchemaConverters
import com.ibm.aardpfark.pfa.document.{PFADocument, ToPFA}
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.sql.types.StructType


object SparkSupport {


  def toPFA(t: Transformer, pretty: Boolean): String = {
    toPFATransformer(t).pfa.toJSON(pretty)
  }

  def toPFA(p: PipelineModel, s: StructType, pretty: Boolean): String = {
    val inputFields = s.map { f => f.copy(nullable = false) }
    val inputSchema = StructType(inputFields)
    val pipelineInput = SchemaBuilder.record(s"Input_${p.uid}")
    val inputAvroSchema = SchemaConverters.convertStructToAvro(inputSchema, pipelineInput, "")
    Merge.mergePipeline(p, inputAvroSchema).toJSON(pretty)
  }

  // testing implicit conversions for Spark ML PipelineModel and Transformer to PFA / JSON

  implicit private[aardpfark] def toPFATransformer(transformer: org.apache.spark.ml.Transformer): ToPFA = {

    val pkg = transformer.getClass.getPackage.getName
    val name = transformer.getClass.getSimpleName
    val pfaPkg = pkg.replace("org.apache", "com.ibm.aardpfark")
    val pfaClass = Class.forName(s"$pfaPkg.PFA$name")

    val ctor = pfaClass.getConstructors()(0)
    ctor.newInstance(transformer).asInstanceOf[ToPFA]
  }
}
