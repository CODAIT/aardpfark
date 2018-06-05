package com.ibm.aardpfark.pfa.expression

import com.ibm.aardpfark.pfa.DSLSuiteBase
import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.document.PFABuilder
import org.apache.avro.SchemaBuilder

class CastsSuite extends DSLSuiteBase {

  test("DSL: Casts") {

//    val fromNull = As[Null]("input", _ => "Null")
    val fromDouble = As[Double]("input", _ => "Double")
    val fromInt = As[Int]("input", _ => "Int")
    val cast = Cast(inputExpr, Seq(fromDouble, fromInt))
    val action = Action(cast)

    val schema = SchemaBuilder.unionOf().doubleType().and().intType().endUnion()

    val pfaDoc = new PFABuilder()
      .withInput(schema)
      .withOutput[String]
      .withAction(action)
      .pfa

    val engine = getPFAEngine(pfaDoc.toJSON())

    val doubleResult = engine.action(engine.jsonInput("""{"double": 1.0}"""))
    assert(doubleResult == "Double")
    val intResult = engine.action(engine.jsonInput("""{"int":1}"""))
    assert(intResult == "Int")
  }

}
