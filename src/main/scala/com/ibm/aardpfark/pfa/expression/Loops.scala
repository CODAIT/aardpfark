package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.dsl.StringExpr
import org.json4s.JValue


trait Loops {

  case class ForEachExpr(v: StringExpr, in: PFAExpression, `do`: Seq[PFAExpression]) extends PFAExpression {
    import org.json4s.JsonDSL._

    override def json: JValue = {
      ("foreach" -> v.s) ~ ("in" -> in.json) ~ ("do" -> `do`.map(_.json))

    }
  }

  object ForEach {
    def apply(variable: StringExpr, in: PFAExpression)(fn: (StringExpr) => Seq[PFAExpression]): ForEachExpr =
      ForEachExpr(variable, in, fn(variable))
  }

}
