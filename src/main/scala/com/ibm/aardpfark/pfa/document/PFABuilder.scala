package com.ibm.aardpfark.pfa.document

import scala.collection.mutable.ArrayBuffer

import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.pfa.types.WithSchema
import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.Schema


class PFABuilder {
  import com.ibm.aardpfark.pfa.dsl._

  private var name: Option[String] = None
  private var meta: Map[String, String] = Map()
  private var input: Schema = null
  private var output: Schema = null
  private val cells = collection.mutable.HashMap[String, Cell[_]]()
  private val action = ArrayBuffer[PFAExpression]()
  private val functions = collection.mutable.HashMap[String, FunctionDef]()

  def withInput(schema: Schema): this.type = {
    input = schema
    this
  }

  def withInput[T](implicit ev: SchemaFor[T]): this.type = withInput(ev())

  def withOutput(schema: Schema): this.type = {
    output = schema
    this
  }

  def withName(name: String): this.type = {
    this.name = Some(name)
    this
  }

  def withMetadata(meta: Map[String, String]): this.type = {
    this.meta = meta
    this
  }

  def withOutput[T](implicit ev: SchemaFor[T]): this.type = withOutput(ev())

  def withCell[T <: WithSchema](name: String, cell: Cell[T]): this.type = {
    cells += name -> cell
    this
  }

  def withCell[T <: WithSchema](namedCell: NamedCell[T]): this.type = {
    cells += namedCell.name -> namedCell.cell
    this
  }

  def withFunction(name: String, fn: FunctionDef): this.type = {
    functions += name -> fn
    this
  }

  def withFunction(namedFn: NamedFunctionDef): this.type = {
    functions += namedFn.name -> namedFn.fn
    this
  }

  def withAction(expr: PFAExpression): this.type = {
    expr match {
      case ExprSeq(s) =>
        action ++= s
      case _ =>
        action += expr
    }
    this
  }

  def pfa: PFADocument = {
    PFADocument(name = name,
      metadata = meta,
      input = input,
      output = output,
      action = action,
      cells = cells.toMap,
      fcns = functions.toMap
    )
  }
}

object PFABuilder {
  def apply(): PFABuilder = new PFABuilder()
}
