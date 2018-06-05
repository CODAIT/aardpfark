package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.document.SchemaSerializer
import com.sksamuel.avro4s.{AvroSchema, SchemaFor, ToSchema}
import org.apache.avro.Schema
import org.json4s.JValue
import org.json4s.JsonAST.JString
import org.json4s.native.JsonMethods.parse

trait New {

  object NewRecord {
    def apply(schema: Schema, init: Map[String, PFAExpression], fullSchema: Boolean = true) =
      NewRecordExpr(schema, init, fullSchema)
  }

  case class NewRecordExpr(schema: Schema, init: Map[String, PFAExpression], fullSchema: Boolean)
    extends PFAExpression {
    import org.json4s.JsonDSL._

    private val s = if (fullSchema) SchemaSerializer.convert(schema) else JString(schema.getFullName)
    override def json: JValue = {
      ("type" -> s) ~ ("new" -> init.mapValues(_.json))
    }
  }

  case class NewArrayExpr(schema: Schema, init: Seq[PFAExpression]) extends PFAExpression {
    import org.json4s.JsonDSL._

    override def json: JValue = {
      ("type" -> parse(schema.toString)) ~ ("new" -> init.map(_.json))
    }
  }

  object NewArray {
    def apply(schema: Schema, init: Seq[PFAExpression]) = NewArrayExpr(schema, init)
    def apply[T](init: Seq[PFAExpression])(implicit s: ToSchema[Seq[T]]) = {
      NewArrayExpr(s(), init)
    }
  }

  case class NewMap(schema: Schema, init: Map[String, PFAExpression]) extends PFAExpression {
    import org.json4s.JsonDSL._

    override def json: JValue = {
      ("type" -> parse(schema.toString)) ~ ("new" -> init.mapValues(_.json))
    }
  }

}

