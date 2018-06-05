package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.DSLSuiteBase
import com.ibm.aardpfark.pfa.document.PFABuilder
import com.ibm.aardpfark.pfa.dsl._
import org.apache.avro.Schema

class LetSetSuite extends DSLSuiteBase {

  test("DSL: Let Set") {
    val sum = Let("var", 0.0)

    val action = Action(sum, Set(sum.ref, inputExpr), sum.ref)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("1.0"))

    assert(result == 1.0)
  }

}
