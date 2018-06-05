package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.document.SchemaSerializer
import com.ibm.aardpfark.pfa.dsl.StringExpr
import com.sksamuel.avro4s.{SchemaFor, ToSchema}
import org.apache.avro.Schema
import org.json4s.JValue

trait Casts {

  case class As(schema: Schema, named: String, `do`: PFAExpression)

  object As {
    def apply(schema: Schema, named: String, `do`: (StringExpr) => PFAExpression): As = {
      As(schema, named, `do`(StringExpr(named)))
    }
    def apply[T](named: String, `do`: (StringExpr) => PFAExpression)(implicit s: ToSchema[T]): As = {
      As(s(), named, `do`(StringExpr(named)))
    }
  }
  object Cast {
    def apply(cast: PFAExpression, cases: Seq[As]) = new CastExpr(cast, cases)
    def apply(cast: PFAExpression, case1: As, cases: As*) = new CastExpr(cast, Seq(case1) ++ cases)
  }

  class CastExpr(cast: PFAExpression, cases: Seq[As]) extends PFAExpression {
    import org.json4s.JsonDSL._

    implicit val converter: Schema => JValue = SchemaSerializer.convert
    override def json: JValue = {
      ("cast" -> cast.json) ~
        ("cases" -> cases.map { as =>
          ("as" -> as.schema) ~ ("named" -> as.named) ~ ("do" -> as.`do`.json)
        })
    }
  }

}
