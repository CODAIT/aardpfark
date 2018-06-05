package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.dsl.ExprSeq
import org.json4s.JValue

object Do {
  def apply(exprs: Seq[PFAExpression]) = DoExpr(exprs)
  def apply(expr: PFAExpression) = DoExpr(Seq(expr))
  def apply(first: PFAExpression, next: PFAExpression*) = DoExpr(Seq(first) ++ next)
}

case class DoExpr(exprs: Seq[PFAExpression]) extends PFAExpression {
  import org.json4s.JsonDSL._

  override def json: JValue = {
    ("do" -> exprs.map(_.json))
  }

}


class ConditionalExpr(ifExpr: PFAExpression, thenExpr: PFAExpression, elseExpr: Option[PFAExpression]) extends PFAExpression {
  import org.json4s.JsonDSL._

  override def json: JValue = {
    ("if" -> ifExpr.json) ~ ("then" -> thenExpr.json) ~ ("else" -> elseExpr.map(_.json))
  }

}

trait ControlStructures {

  object If {
    def apply(expr: => PFAExpression) = new IfExpectsThen(expr)

    class IfExpectsThen(ifExpr: PFAExpression) {
      def Then(thenExpr: => PFAExpression) = new IfThenMayExpectElse(ifExpr, thenExpr)
    }

    class IfThenMayExpectElse(ifExpr: PFAExpression, thenExpr: PFAExpression) {
      def expr: PFAExpression = new ConditionalExpr(ifExpr, thenExpr, None)
      def Else(elseExpr: => PFAExpression) = new ConditionalExpr(ifExpr, thenExpr, Some(elseExpr))
    }

    implicit def onlyThen(cond: IfThenMayExpectElse): PFAExpression = cond.expr
  }

}
