package com.ibm.aardpfark.pfa.document

import scala.util.Try

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.spark.ml.tree.{TreeNode, Trees}
import org.apache.avro.Schema
import org.json4s.native.JsonMethods.parse
import org.json4s.{CustomSerializer, JValue}


object SchemaSerializer {

  def convert(s: Schema): JValue = {
    import Schema.Type._
    import org.json4s.JsonDSL._
    s.getType match {
      case DOUBLE | FLOAT | INT | LONG | STRING | BOOLEAN | BYTES | NULL  =>
        ("type" -> s.getType.getName)
      case _ =>
        parse(s.toString)
    }
  }
}

class SchemaSerializer extends CustomSerializer[Schema](format => (
  {
    case j: JValue =>
      new Schema.Parser().parse(j.toString)
  },
  {
    case s: Schema =>
      SchemaSerializer.convert(s)
  }
)
)

class PFAExpressionSerializer extends CustomSerializer[PFAExpression](format => (
  {
    case j: JValue =>
      throw new UnsupportedOperationException("cannot deserialize")
  },
  {
    case expr: PFAExpression =>
      expr.json
  }
)
)

class TreeSerializer extends CustomSerializer[TreeNode](format => (
  {
    case j: JValue =>
      throw new UnsupportedOperationException("cannot deserialize")
  },
  {
    case tree: TreeNode =>
      Trees.json(tree)
  }
)
)

class ParamSerializer extends CustomSerializer[Param](format => (
  {
    case j: JValue =>
      throw new UnsupportedOperationException("cannot deserialize")
  },
  {
    case p: Param =>
      import org.json4s.JsonDSL._
      if (p.simpleSchema) {
        (p.name -> p.`type`.getFullName)
      } else {
        val schemaSerializer = new SchemaSerializer().serialize(format)
        (p.name -> schemaSerializer(p.`type`))
      }

  }
)
)