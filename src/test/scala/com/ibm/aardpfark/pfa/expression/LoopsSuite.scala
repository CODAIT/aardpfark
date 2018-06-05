package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.DSLSuiteBase
import com.ibm.aardpfark.pfa.document.PFABuilder
import com.ibm.aardpfark.pfa.dsl._
import org.apache.avro.Schema

class LoopsSuite extends DSLSuiteBase {

  test("DSL: For loop") {
    val sum = Let("sum", 0.0)

    val foreach = ForEach(StringExpr("element"), inputExpr) {
      e => Seq(Set(sum.ref, core.plus(sum.ref, e)))
    }
    val action = Action(sum, foreach, sum.ref)

    val pfaDoc = new PFABuilder()
      .withInput(Schema.createArray(Schema.create(Schema.Type.DOUBLE)))
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[3.0, 4.0, 5.0]"))

    assert(result == 12.0)
  }

}
