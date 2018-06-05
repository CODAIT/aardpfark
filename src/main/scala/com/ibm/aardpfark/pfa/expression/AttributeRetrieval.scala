package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.types.WithSchema
import org.json4s.{JField, JObject, JValue}


private[pfa] class CellRetrieval[T <: WithSchema](name: String, path: Seq[PFAExpression] = Seq())
  extends PFAExpression {
  import org.json4s.JsonDSL._

  override def json: JValue = {
    JObject(JField("cell", name), JField("path", path.map(_.json)))
  }
}

private[pfa] class AttrRetrieval(attr: PFAExpression, path: Seq[PFAExpression] = Seq())
  extends PFAExpression {
  import org.json4s.JsonDSL._

  override def json: JValue = {
    JObject(JField("attr", attr.json), JField("path", path.map(_.json)))
  }
}

trait AttributeRetrieval {

  object Attr {
    def apply(ref: PFAExpression, path: PFAExpression*) = new AttrRetrieval(ref, path)
  }

}
