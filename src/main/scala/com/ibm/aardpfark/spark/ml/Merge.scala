package com.ibm.aardpfark.spark.ml

import com.ibm.aardpfark.pfa.document.Cell
import com.ibm.aardpfark.pfa.expression._
import org.apache.avro.{Schema, SchemaBuilder}

import org.apache.spark.ml.PipelineModel

/*
  TODO: this is a rough, brittle initial version of pipeline support that needs to be improved.
 */
object Merge {

  import collection.JavaConversions._

  import com.ibm.aardpfark.pfa.dsl._
  import com.ibm.aardpfark.spark.ml.SparkSupport._

  def mergePipeline(pipeline: PipelineModel, is: Schema) = {

    val docs = pipeline.stages.map(_.pfa)

    /*
      Merge requires:

      1. Left input => merged input
      2. Left output => right input
      3. Right output => merged output
      4. Merge metadata
      5. Merge cells
      6. Merge functions (with rename)
      7. Create the relevant actions
     */
    val first = docs.head
    val last = docs.last
    var name = "merged"
    var version = 0L
    val inputSchema = is
    val outputSchema = last.output
    var meta: Map[String, String] = Map()
    var cells: Map[String, Cell[_]] = Map()
    var action: PFAExpression = StringExpr("input")
    var fcns: Map[String, FunctionDef] = Map()
    var currentSchema = inputSchema

    docs.zipWithIndex.foreach { case (doc, idx) =>

      val inputParam = Param("input", currentSchema)

      val inputFields = currentSchema.getFields.toSeq
      val newFields = doc.output.getFields.toSeq
      val outputFields = inputFields ++ newFields

      val bldr = SchemaBuilder.record(s"Stage_${idx + 1}_output_schema").fields()
      outputFields.foreach { field =>
        bldr
          .name(field.name())
          .`type`(field.schema())
          .noDefault()
      }

      currentSchema = bldr.endRecord()

      val let = Let(s"Stage_${idx + 1}_action_output", Do(doc.action))
      val inputExprs = inputFields.map { field =>
        field.name -> StringExpr(s"input.${field.name}")
      }
      val newExprs = newFields.map { field =>
        field.name -> StringExpr(s"${let.x}.${field.name}")

      }
      val exprs = inputExprs ++ newExprs
      val stageOutput = NewRecord(currentSchema, exprs.toMap)

      val le = new LetExpr(Seq((let.x, let.`type`, let.expr)))

      val stageActionFn = NamedFunctionDef(s"Stage_${idx + 1}_action", FunctionDef(
        Seq(inputParam), currentSchema, Seq(le, stageOutput)
      ))

      fcns = fcns ++ doc.fcns + (stageActionFn.name -> stageActionFn.fn)
      cells = cells ++ doc.cells
      meta = meta ++ doc.metadata
      action = stageActionFn.call(action)
    }

    first.copy(
      name = Some(name),
      version = Some(version),
      metadata = meta,
      cells = cells,
      fcns = fcns,
      action = action,
      input = inputSchema,
      output = currentSchema
    )

  }
}