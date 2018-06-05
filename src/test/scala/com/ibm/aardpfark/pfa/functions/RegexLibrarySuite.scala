package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.document.PFABuilder

class RegexLibrarySuite extends FunctionLibrarySuite {

  test("Regex split") {
    val action = re.split(inputExpr, " ")

    val pfaDoc = new PFABuilder()
      .withInput[String]
      .withOutput(stringArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("\"test string\""))
    assert(engine.jsonOutput(result) == "[\"test\",\"string\"]")
  }

}
