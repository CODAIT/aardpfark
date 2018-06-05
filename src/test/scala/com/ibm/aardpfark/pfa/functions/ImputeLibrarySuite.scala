package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.document.PFABuilder

class ImputeLibrarySuite extends FunctionLibrarySuite {
  import com.ibm.aardpfark.pfa.dsl._

  test("Impute nan") {
    val action = impute.isnan(core.div(0.0, inputExpr))

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("1.0")) == false)
    assert(engine.action(engine.jsonInput("0.0")) == true)
  }

}
