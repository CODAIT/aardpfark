package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.document.PFABuilder

class StringLibrarySuite extends FunctionLibrarySuite {

  test("String lower") {
    val action = s.lower(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[String]
      .withOutput[String]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("\"UPPER\"")) == "upper")
  }

  test("String join") {
    val action = s.join(inputExpr, " ")

    val pfaDoc = new PFABuilder()
      .withInput(stringArraySchema)
      .withOutput[String]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("""["a", "b", "c"]""")) == "a b c")
  }

}
