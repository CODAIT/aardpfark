package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.document.PFABuilder
import com.ibm.aardpfark.pfa.dsl._

class CoreLibrarySuite extends FunctionLibrarySuite {

  test("Core plus") {
    val action = core.plus(inputExpr, 1.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("0.5")) == 1.5)
  }

  test("Core minus") {
    val action = core.minus(inputExpr, 0.5)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("2.0")) == 1.5)
  }

  test("Core mult") {
    val action = core.mult(inputExpr, 2.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("0.5")) == 1.0)
  }

  test("Core div") {
    val action = core.div(inputExpr, 2.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("4.0")) == 2.0)
  }

  test("Core addinv") {
    val action = core.addinv(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("-1.0")) == 1.0)
  }

  test("Core and") {
    val action = core.and(inputExpr, inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Boolean]
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("true")) == true)
  }

  test("Core eq") {
    val action = core.eq(inputExpr, 1.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("1.0")) == true)
  }

  test("Core lt") {
    val action = core.lt(inputExpr, 2.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("1.0")) == true)
  }

  test("Core lte") {
    val action = core.lte(inputExpr, 2.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("1.0")) == true)
  }

  test("Core gt") {
    val action = core.gt(inputExpr, 1.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("2.0")) == true)
  }

  test("Core gte") {
    val action = core.gte(inputExpr, 1.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("2.0")) == true)
  }

  test("Core net") {
    val action = core.net(inputExpr, 1.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("2.0")) == true)
  }

  test("Core pow") {
    val action = core.pow(inputExpr, 3.0)

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("2.0")) == 8.0)
  }

  test("Core not") {
    val action = core.not(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput[Boolean]
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("false")) == true)
  }

}
