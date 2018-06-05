package com.ibm.aardpfark

import com.ibm.aardpfark.pfa.expression._
import org.json4s.{JString, JValue}


package object pfa {

  object dsl extends expression.API
    with types.API
    with functions.API {

    implicit def SeqToExpr(seq: Seq[PFAExpression]) = ExprSeq(seq)
    implicit def singleExprToSeq(expr: PFAExpression) = Seq(expr)

    case class StringExpr(s: String) extends PFAExpression {
      override def json: JValue = JString(s)
    }

    case class ExprSeq(exprs: Seq[PFAExpression]) extends PFAExpression {
      import org.json4s.JsonDSL._

      override def json: JValue = {
        exprs.map(_.json)
      }

    }

    case class ErrorExpr(msg: String) extends PFAExpression {
      import org.json4s.JsonDSL._

      override def json: JValue = ("error" -> msg)
    }

  }

}
