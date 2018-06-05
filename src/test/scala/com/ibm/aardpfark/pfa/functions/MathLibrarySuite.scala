package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.document.PFABuilder
import org.scalactic.Tolerance._
import org.json4s._
import org.json4s.native.JsonMethods._

class MathLibrarySuite extends FunctionLibrarySuite {

  test("Math abs") {
    val action = m.abs(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("-0.5")) == 0.5)
  }

  test("Math exp") {
    val action = m.exp(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.jsonOutput(engine.action(engine.jsonInput("2"))).toDouble
    assert(result === 7.3890560989 +- 1e-6)
  }

  test("Math ln") {
    val action = m.ln(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.jsonOutput(engine.action(engine.jsonInput("2"))).toDouble
    assert(result === 0.6931471806 +- 1e-6)
  }

  test("Math sqrt") {
    val action = m.sqrt(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("4.0")) == 2.0)
  }

  test("m.link logit") {
    val action = m.link.logit(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("1"))
    assert(result.toString.toDouble === 0.73105 +- tol)
  }

  test("m.link softmax") {
    val action = m.link.softmax(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val pfaResult = engine.action(engine.jsonInput("[1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0]"))

    val parsedResult = parse(pfaResult.toString).extract[List[Double]]
    val expectedResult = List(0.024, 0.064, 0.175, 0.475, 0.024, 0.064, 0.175)

    parsedResult.zip(expectedResult).foreach { case (actual, expected) =>
        assert(actual === expected +- tol)
    }
  }

}
