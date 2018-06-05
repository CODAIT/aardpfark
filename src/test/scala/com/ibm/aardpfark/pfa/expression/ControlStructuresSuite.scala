package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.DSLSuiteBase
import com.ibm.aardpfark.pfa.document.PFABuilder
import com.ibm.aardpfark.pfa.dsl._
import org.apache.avro.Schema

class ControlStructuresSuite extends DSLSuiteBase {

  test("DSL: If-then statements") {
    val action = If {core.gt(inputExpr, 0.0)} Then "Positive" Else "Negative"

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[String]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())

    assert("Positive" == engine.action(engine.jsonInput("1.0")))
    assert("Negative" == engine.action(engine.jsonInput("-1.0")))
  }

}
