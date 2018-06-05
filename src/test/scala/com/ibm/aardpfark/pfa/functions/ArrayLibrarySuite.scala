package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.document.PFABuilder
import org.apache.avro.Schema
import org.json4s.native.JsonMethods.parse

class ArrayLibrarySuite extends FunctionLibrarySuite {

  test("Array argmax") {
    val action = a.argmax(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("[0.0, 1.0, 2.0, 3.0]")) == 3.0)
  }

  test("Array append") {
    val action = a.append(inputExpr, 4.0)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[0.0, 1.0, 3.0]"))
    assert(engine.jsonOutput(result) == "[0.0,1.0,3.0,4.0]")
  }

  test("Array replace") {
    val action = a.replace(inputExpr, 0, 4.0)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[0.0, 1.0, 3.0]"))
    assert(engine.jsonOutput(result) == "[4.0,1.0,3.0]")
  }

  test("Array contains") {
    val action = a.contains(inputExpr, 3.0)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("[0.0, 1.0, 3.0]")) == true)
    assert(engine.action(engine.jsonInput("[0.0, 1.0, 2.0]")) == false)
  }

  test("Array filter") {
    val gteFn = FunctionDef[Double, Boolean]("num") { e => Seq(core.gte(e, 1.0))}
    val action = a.filter(inputExpr, gteFn)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[0.0, 1.0, 3.0]"))
    assert(engine.jsonOutput(result) == "[1.0,3.0]")
  }

  test("Array filterWithIndex") {
    val eqFn = FunctionDef[Boolean](Seq(Param[Int]("idx"), Param[String]("word")), Seq(core.eq(StringExpr("idx"), 0)))
    val action = a.filterWithIndex(inputExpr, eqFn)

    val pfaDoc = new PFABuilder()
      .withInput(stringArraySchema)
      .withOutput(stringArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[\"the\", \"dog\"]"))
    assert(engine.jsonOutput(result) == "[\"the\"]")
  }

  test("Array flatten") {
    val action = a.flatten(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArrayArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[[0.0, 1.0], [3.0, 2.0, 1.0], [1.0]]"))
    assert(engine.jsonOutput(result) == "[0.0,1.0,3.0,2.0,1.0,1.0]")
  }

  test("Array zipmap") {
    val plusFn = NamedFunctionDef("plus", FunctionDef[Double, Double]("x", "y") { case Seq(x, y) => core.plus(x, y) })
    val action = a.zipmap(plusFn.ref, inputExpr, inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .withFunction(plusFn)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())

    val result = engine.action(engine.jsonInput("[0.0, 1.0, 3.0]"))
    assert(engine.jsonOutput(result) == "[0.0,2.0,6.0]")
  }

  test("Array map") {
    val inverseFn = FunctionDef[Double, Double]("x") { x => Seq(core.addinv(x)) }
    val action = a.map(inputExpr, inverseFn)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())

    val result = engine.action(engine.jsonInput("[0.0, 1.0, 3.0]"))
    assert(engine.jsonOutput(result) == "[-0.0,-1.0,-3.0]")
  }

  test("Array max") {
    val action = a.max(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("[0.0, 1.0, 3.0]")) == 3.0)
  }

  test("Array mode") {
    val action = a.mode(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("[0.0, 1.0, 1.0, 3.0]")) == 1.0)
  }

  test("Array mean") {
    val action = a.mean(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("[0.0, 0.0, 1.0, 3.0]")) == 1.0)
  }

  test("Array sum") {
    val action = a.sum(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput[Double]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("[0.0, 1.0, 3.0]")) == 4.0)
  }

  test("Array slidingWindow") {
    val action = a.slidingWindow(inputExpr, 2, 1)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(Schema.createArray(doubleArraySchema))
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[0.0, 1.0, 3.0]"))
    val parsedResult = parse(result.toString).extract[List[List[Double]]]
    val expectedResult = List(List(0.0, 1.0), List(1.0, 3.0))

    parsedResult.zip(expectedResult).foreach { case (actual, expected) =>
      assert(actual == expected)
    }
  }

  test("Array len") {
    val action = a.len(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput[Int]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("[0.0, 1.0, 3.0]")) == 3)
  }

  test("Array index") {
    val eqFn = NamedFunctionDef("equals", FunctionDef[Double, Boolean]("num") { e =>
      Seq(core.eq(e, 3.0))
    })
    val action = a.index(inputExpr, eqFn.ref)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput[Int]
      .withAction(action)
      .withFunction(eqFn)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("[1.0, 3.0, 2.0]")) == 1)
  }

  test("Array subseq") {
    val action = a.subseq(inputExpr, 0, 2)

    val pfaDoc = new PFABuilder()
      .withInput(doubleArraySchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("[0.0, 1.0, 3.0]"))
    assert(engine.jsonOutput(result) == "[0.0,1.0]")
  }

}
