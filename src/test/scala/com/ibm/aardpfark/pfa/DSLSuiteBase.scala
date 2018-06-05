package com.ibm.aardpfark.pfa

import com.ibm.aardpfark.pfa.dsl.StringExpr
import org.apache.avro.Schema
import org.scalatest.FunSuite

abstract class DSLSuiteBase extends FunSuite with PFATestUtils {

  val inputExpr = StringExpr("input")

  val doubleArraySchema = Schema.createArray(Schema.create(Schema.Type.DOUBLE))
  val doubleArrayArraySchema = Schema.createArray(doubleArraySchema)
  val stringArraySchema = Schema.createArray(Schema.create(Schema.Type.STRING))
  val intMapSchema = Schema.createMap(Schema.create(Schema.Type.INT))
  val doubleMapSchema = Schema.createMap(Schema.create(Schema.Type.DOUBLE))
  val stringMapSchema = Schema.createMap(Schema.create(Schema.Type.STRING))
}
