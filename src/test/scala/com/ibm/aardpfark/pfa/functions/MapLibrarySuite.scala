package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.document.PFABuilder
import org.json4s.native.JsonMethods.parse

class MapLibrarySuite extends FunctionLibrarySuite {

  test("Map add") {
    val action = map.add(inputExpr, StringLiteral("two"), 2)

    val pfaDoc = new PFABuilder()
      .withInput(intMapSchema)
      .withOutput(intMapSchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("{\"one\": 1}"))
    assert(engine.jsonOutput(result) == """{"one":1,"two":2}""")
  }

  test("Map containsKey") {
    val action = map.containsKey(inputExpr, "one")

    val pfaDoc = new PFABuilder()
      .withInput(doubleMapSchema)
      .withOutput[Boolean]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    assert(engine.action(engine.jsonInput("{\"one\": 1}")) == true)
  }

  test("Map filter") {
    val gteFn = NamedFunctionDef("gte", FunctionDef[Double, Boolean]("num") { e => Seq(core.gte(e, 1.0))})
    val action = map.filter(inputExpr, gteFn.ref)

    val pfaDoc = new PFABuilder()
      .withInput(doubleMapSchema)
      .withOutput(doubleMapSchema)
      .withAction(action)
      .withFunction(gteFn)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("""{"one": 1, "negtwo": -2}"""))
    assert(engine.jsonOutput(result) == """{"one":1.0}""")
  }

  test("Map map") {
    val adder = NamedFunctionDef("adder", FunctionDef[Int, Int]("num") { e => Seq(core.plus(e, 1))})
    val action = map.map(inputExpr, adder.ref)

    val pfaDoc = new PFABuilder()
      .withInput(intMapSchema)
      .withOutput(intMapSchema)
      .withAction(action)
      .withFunction(adder)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("{\"one\": 1,\"two\": 2}"))
    assert(engine.jsonOutput(result) == """{"one":2,"two":3}""")
  }

  test("Map mapWithKey") {
    val adder = NamedFunctionDef("adder", FunctionDef[String](Seq(Param[String]("key"), Param[String]("val")))
      { case Seq(k, v) => {
        val strings = NewArray(stringArraySchema, Seq(k.ref, v.ref))
        Seq(s.join(strings, ""))
      }})
    val action = map.mapWithKey(inputExpr, adder.ref)

    val pfaDoc = new PFABuilder()
      .withInput(stringMapSchema)
      .withOutput(stringMapSchema)
      .withAction(action)
      .withFunction(adder)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("""{"dog":"house","bird":"nest"}"""))
    val parsedResult = parse(engine.jsonOutput(result)).extract[Map[String,String]]
    val expectedResult = Map(("dog","doghouse"), ("bird", "birdnest"))
    assert(parsedResult == expectedResult)
  }

  test("Map keys") {

    val action = map.keys(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleMapSchema)
      .withOutput(stringArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("{\"zero\": 0.0,\"two\": 2.0}"))
    assert(engine.jsonOutput(result) == """["zero","two"]""")
  }

  test("Map values") {

    val action = map.values(inputExpr)

    val pfaDoc = new PFABuilder()
      .withInput(doubleMapSchema)
      .withOutput(doubleArraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("{\"zero\": 0.0,\"two\": 2.0}"))
    assert(engine.jsonOutput(result) == "[0.0,2.0]")
  }


  test("Map zipmap") {
    val plusFn = NamedFunctionDef("plusFn",
      FunctionDef[Int](Seq(Param[Int]("x"), Param[Int]("y")), Seq(core.plus("x", "y"))))
    val action = map.zipmap(inputExpr, inputExpr, plusFn.ref)

    val pfaDoc = new PFABuilder()
      .withInput(intMapSchema)
      .withOutput(intMapSchema)
      .withAction(action)
      .withFunction(plusFn)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())
    val result = engine.action(engine.jsonInput("{\"one\": 1,\"two\": 2}"))
    assert(engine.jsonOutput(result) == """{"one":2,"two":4}""")
  }

}
