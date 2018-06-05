package com.ibm.aardpfark.pfa

import com.ibm.aardpfark.pfa.expression.PFAExpression
import org.json4s.JsonAST.{JBool, JLong}
import org.json4s.{JDouble, JField, JInt, JNull, JObject, JString, JValue}
import org.json4s.JsonDSL._

package object types {
  private[pfa] trait API {

    sealed trait Literal extends PFAExpression

    implicit class StringLiteral(s: String) extends Literal {
      override def json: JValue = "string" -> s
    }

    implicit class DoubleLiteral(d: Double) extends Literal {
      override def json: JValue = "double" -> d
    }

    implicit class FloatLiteral(f: Float) extends Literal {
      override def json: JValue = "float" -> f
    }

    implicit class IntLiteral(i: Int) extends Literal {
      override def json: JValue = "int" -> i
    }

    implicit class LongLiteral(l: Long) extends Literal {
      override def json: JValue = "long" -> l
    }

    implicit class BooleanLiteral(b: Boolean) extends Literal {
      override def json: JValue = b
    }

    case object NullLiteral extends Literal {
      override def json: JValue = JNull
    }

  }
}
