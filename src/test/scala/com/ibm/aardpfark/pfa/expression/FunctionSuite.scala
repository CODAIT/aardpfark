package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.DSLSuiteBase
import com.ibm.aardpfark.pfa.document.PFABuilder
import org.apache.avro.Schema

class FunctionSuite extends DSLSuiteBase {

  test("DSL: NamedFunctionDef") {

    val squared = FunctionDef[Int, Int]("x") { x =>
      Seq(core.mult(x, x))
    }
    val namedSquared = NamedFunctionDef("squared", squared)

    val cubed = FunctionDef[Int, Int]("x") {x =>
      Seq(core.mult(x, namedSquared(x)))
    }
    val namedCubed = NamedFunctionDef("cubed", cubed)

    val action = Action(namedSquared(namedCubed(inputExpr)))

    val pfaDoc = new PFABuilder()
      .withInput[Int]
      .withOutput[Int]
      .withAction(action)
      .withFunction(namedSquared)
      .withFunction(namedCubed)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())

    assert(1 == engine.action(engine.jsonInput("1")))
    assert(64 == engine.action(engine.jsonInput("2")))
  }

  test("DSL: FunctionDef anonymous"){
    val squared = FunctionDef[Int, Int]("x") { x =>
      Seq(core.mult(x, x))
    }

    val arraySchema = Schema.createArray(Schema.create(Schema.Type.INT))

    val action = Action(
      a.map(inputExpr, squared)
    )

    val pfaDoc = new PFABuilder()
      .withInput(arraySchema)
      .withOutput(arraySchema)
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())

    assert("[1,4,9]" == engine.jsonOutput(engine.action(engine.jsonInput("[1,2,3]"))))
    assert("[1,4,9]" == engine.jsonOutput(engine.action(engine.jsonInput("[-1,-2,3]"))))
    assert("[9,64,256]" == engine.jsonOutput(engine.action(engine.jsonInput("[3,8,16]"))))
  }

  test("DSL: FunctionDef multiple args with same input type") {
    val fn = NamedFunctionDef("plusAll", FunctionDef[Double, Double]("x", "y", "z") {
      case Seq(x, y, z) =>
        core.plus(core.plus(x, y), z)
    })

    val action = Action(fn.call(inputExpr, core.mult(inputExpr, 2.0), 6.0))

    val pfaDoc = new PFABuilder()
      .withInput[Double]
      .withOutput[Double]
      .withAction(action)
      .withFunction(fn)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())

    assert(12.0 == engine.action(engine.jsonInput("2.0")))
  }

}
