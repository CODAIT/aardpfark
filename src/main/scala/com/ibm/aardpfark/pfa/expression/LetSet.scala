package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.dsl.StringExpr
import org.json4s.JValue


trait LetSet {

  type LetArgs = (String, Option[String], PFAExpression)

  // implicits
  implicit def LetToLetExpr(let: Let) = new LetExpr(Seq((let.x, let.`type`, let.expr)))
  implicit def LetsToLetExpr(lets: Seq[Let]) = {
    new LetExpr(lets.map(let => (let.x, let.`type`, let.expr)))
  }

  case class Let(x: String, `type`: Option[String], expr: PFAExpression) {
    def ref = StringExpr(x)
  }

  object Let {
    def apply(ref: String, expr: PFAExpression): Let = Let(ref, None, expr)
  }

  object Set {
    def apply(ref: StringExpr, expr: PFAExpression): SetExpr = new SetExpr(ref, expr)
  }


  class LetExpr(args: Seq[LetArgs]) extends PFAExpression {
    import org.json4s.JsonDSL._

    override def json: JValue = {
      val lets = args.map { case (name, schema, init) =>
        schema.map { s =>
          (name -> ("type" -> s) ~ ("value" -> init.json))
        }.getOrElse {
          (name -> init.json)
        }
      }.toMap
      ("let" -> lets)
    }
  }

  class SetExpr(ref: StringExpr, expr: PFAExpression) extends PFAExpression {
    import org.json4s.JsonDSL._

    override def json: JValue = {
      ("set" -> (ref.s -> expr.json))
    }
  }

}
