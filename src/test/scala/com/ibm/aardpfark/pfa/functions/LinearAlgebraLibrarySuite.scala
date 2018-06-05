package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.document.PFABuilder
import org.apache.avro.Schema

class LinearAlgebraLibrarySuite extends FunctionLibrarySuite {

  test("Linear algebra add") {
    val action = la.add(inputExpr, NewArray[Double](Seq(-1.0, 1.0, 4.0)))

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[1.0, 10.0, -3.0]"))
    assert(engine.jsonOutput(result) == "[0.0,11.0,1.0]")
  }

  test("Linear algebra dot - matrix / matrix") {
    val action = la.dot(inputExpr, inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(Schema.createArray(doubleArraySchema))
      .withOutput(Schema.createArray(doubleArraySchema))
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[[0.0, 1.0], [2.0, 1.0]]"))
    assert(engine.jsonOutput(result) == "[[2.0,1.0],[2.0,3.0]]")
  }

  test("Linear algebra dot - matrix / vector") {
    val action = la.dot(inputExpr, NewArray[Double](Seq(-1.0, 1.0)))

    val pfaDoc = new PFABuilder()
      .withInput(Schema.createArray(doubleArraySchema))
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[[0.0, 1.0], [2.0, 1.0]]"))
    assert(engine.jsonOutput(result) == "[1.0,-1.0]")
  }

  test("Linear algebra scale") {
    val action = la.scale(inputExpr, 0.5)

    val pfaDoc = new PFABuilder()
      .withInput(Schema.createArray(doubleArraySchema))
      .withOutput(Schema.createArray(doubleArraySchema))
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[[0.0, 1.0], [2.0, 1.0]]"))
    assert(engine.jsonOutput(result) == "[[0.0,0.5],[1.0,0.5]]")
  }

  test("Linear algebra sub") {
    val action = la.sub(inputExpr, NewArray[Double](Seq(-1.0, 1.0, 4.0)))

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[1.0, 10.0, -3.0]"))
    assert(engine.jsonOutput(result) == "[2.0,9.0,-7.0]")
  }

}
