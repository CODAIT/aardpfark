package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.document.{PFAExpressionSerializer, ParamSerializer, SchemaSerializer}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import org.json4s.{JDouble, JField, JInt, JObject, JString, JValue, NoTypeHints}


class FunctionCall(name: String, args: Any*) extends PFAExpression {
  import com.ibm.aardpfark.pfa.dsl._
  import org.json4s.JsonDSL._

  override def json: JValue = {
    val jArgs = args.map {
      case n: Double =>
        JDouble(n)
      case i: Int =>
        JInt(i)
      case s: String =>
        JString(s)
      case expr: PFAExpression =>
        expr.json
      case fnDef: FunctionDef =>
        implicit val formats = Serialization.formats(NoTypeHints) +
          new SchemaSerializer +
          new PFAExpressionSerializer +
          new ParamSerializer
        parse(write(fnDef))
    }
    JObject(JField(name, jArgs) :: Nil)
  }
}
