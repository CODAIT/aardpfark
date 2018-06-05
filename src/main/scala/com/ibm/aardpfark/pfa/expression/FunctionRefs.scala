package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.dsl.StringExpr
import com.sksamuel.avro4s.{SchemaFor, ToSchema}
import org.apache.avro.Schema
import org.json4s.JValue


class FunctionRef(fcn: String) extends PFAExpression {
  import org.json4s.JsonDSL._

  override def json: JValue = "fcn" -> fcn
}

class PartialFunctionRef(fcn: String, args: Seq[(String, PFAExpression)]) extends FunctionRef(fcn) {
  import org.json4s.JsonDSL._

  override def json: JValue = {
    ("fcn" -> fcn) ~ ("fill" -> args.toMap.mapValues(_.json))
  }

}



trait FunctionRefs {

  case class Param(name: String, `type`: Schema, val simpleSchema: Boolean = false) {
    def ref = StringExpr(name)
  }

  object Param {
    def apply[T](name: String)(implicit s: ToSchema[T]): Param = Param(name, s())
    def apply[T](name: String, simpleSchema: Boolean)(implicit s: ToSchema[T]): Param = Param(name, s(), simpleSchema)
  }

  case class NamedFunctionDef(name: String, fn: FunctionDef) {
    private val _name = s"u.$name"

    def ref = new FunctionRef(_name)

    def apply(args: Any*) = new FunctionCall(_name, args: _*)

    def call(args: Any*) = new FunctionCall(_name, args: _*)

    def fill(arg: PFAExpression, args: PFAExpression*) = {
      val allArgs = arg +: args
      val exprs = fn.params.zip(allArgs).map { case (param, expr) => (param.name, expr) }
      new PartialFunctionRef(_name, exprs)
    }

    def fill(arg: (String, PFAExpression), args: (String, PFAExpression)*) = {
      val exprs = args
      new PartialFunctionRef(_name, exprs)
    }

    def fill(args: Map[String, PFAExpression]) = {
      val exprs = args.toSeq
      new PartialFunctionRef(_name, exprs)
    }

  }

  object FunctionDef {
    def apply[I, O](params: Seq[String], `do`: Seq[PFAExpression])
      (implicit is: ToSchema[I], os: ToSchema[O]): FunctionDef =  {
      FunctionDef(params.map(Param[I](_)), os(), `do`)
    }

    def apply[I, O](param: String, `do`: PFAExpression*)
      (implicit is: ToSchema[I], os: ToSchema[O]): FunctionDef =  {
      FunctionDef(Seq(Param[I](param)), os(), `do`)
    }

    def apply[I, O](param: String)(exprBlock: StringExpr => Seq[PFAExpression])
      (implicit is: ToSchema[I], os: ToSchema[O]): FunctionDef =  {
      FunctionDef(Seq(Param[I](param)), os(), exprBlock(StringExpr(param)))
    }

    def apply[I, O](params: String*)(exprBlock: Seq[StringExpr] => Seq[PFAExpression])
      (implicit is: ToSchema[I], os: ToSchema[O]): FunctionDef = {
      FunctionDef(params.map(Param[I](_)), os(), exprBlock(params.map(StringExpr(_))))
    }

    def apply[O](params: Seq[Param], `do`: Seq[PFAExpression])
      (implicit s: ToSchema[O]): FunctionDef = {
      FunctionDef(params, s(), `do`)
    }

    def apply[O](params: Seq[Param])(exprBlock: Seq[Param] => Seq[PFAExpression])
      (implicit s: ToSchema[O]): FunctionDef =  {
      FunctionDef(params, s(), exprBlock(params))
    }

    def apply[O](param: Param, `do`: Seq[PFAExpression])
      (implicit s: ToSchema[O]): FunctionDef =  {
      FunctionDef(Seq(param), s(), `do`)
    }

    def apply(param: Param, ret: Schema, `do`: Seq[PFAExpression]): FunctionDef =  {
      FunctionDef(Seq(param), ret, `do`)
    }
  }

  case class FunctionDef(params: Seq[Param], ret: Schema, `do`: Seq[PFAExpression])

}


