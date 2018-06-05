package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.document.PFABuilder

class CastLibrarySuite extends FunctionLibrarySuite {

  test("Cast json") {
    val action = cast.json(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput[String]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("[0.0, 1.0, 3.0]")) == "[0.0,1.0,3.0]")
  }

  test("Cast int") {
    val action = cast.int(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Int]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("3.0")) == 3)
  }

  test("Cast double") {
    val action = cast.double(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Int]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("3")) == 3.0)
  }

}
