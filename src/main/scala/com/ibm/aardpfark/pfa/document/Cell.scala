package com.ibm.aardpfark.pfa.document

import com.ibm.aardpfark.pfa.expression.{CellRetrieval, PFAExpression}
import com.ibm.aardpfark.pfa.types.WithSchema

case class NamedCell[T <: WithSchema](name: String, cell: Cell[T]) {
  def ref: CellRetrieval[T] = new CellRetrieval(name)
  def ref(path: PFAExpression*): CellRetrieval[T] = new CellRetrieval(name, path)
}

// simple cell defn
case class Cell[T <: WithSchema](init: T) {
  val `type` = init.schema
}

