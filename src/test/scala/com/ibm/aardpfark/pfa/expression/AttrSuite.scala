package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.DSLSuiteBase
import com.ibm.aardpfark.pfa.document.PFABuilder
import org.apache.avro.Schema

class AttrSuite extends DSLSuiteBase {

  test("DSL: Attr") {

    val action = Attr(Attr(inputExpr, "element"), 1)
    val pfaDoc = new PFABuilder()
      .withInput(Schema.createMap(Schema.createArray(Schema.create(Schema.Type.DOUBLE))))
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("""{"element": [0.0, 3.0]}"""))

    assert(result == 3.0)
  }
}
