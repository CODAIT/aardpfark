package com.ibm.aardpfark.pfa

import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import org.json4s.DefaultFormats

import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.sql.types.StructType


abstract class SparkPipelinePFASuiteBase[A <: Result](implicit m: Manifest[A])
  extends SparkPredictorPFASuiteBase[A] {
  import com.ibm.aardpfark.spark.ml.SparkSupport._

  protected val schema: StructType

  override protected def transformerToPFA(t: Transformer, pretty: Boolean): String = {
    toPFA(t.asInstanceOf[PipelineModel], schema, pretty)
  }
}

abstract class SparkFeaturePFASuiteBase[A <: Result](implicit m: Manifest[A])
  extends SparkPFASuiteBase {

  implicit val formats = DefaultFormats

  protected var isDebug = false

  import com.ibm.aardpfark.spark.ml.SparkSupport._
  import org.json4s._
  import org.json4s.native.JsonMethods._

  test("PFA transformer produces the same results as Spark transformer") {
    parityTest(sparkTransformer, input, expectedOutput)
  }

  protected def transformerToPFA(t: Transformer, pretty: Boolean): String = {
    toPFA(t, pretty)
  }

  protected def testInputVsExpected(
      engine: PFAEngine[AnyRef, AnyRef],
      input: Array[String],
      expectedOutput: Array[String]) = {
    import ApproxEquality._
    input.zip(expectedOutput).foreach { case (in, out) =>
      val pfaResult = engine.action(engine.jsonInput(in))
      val actual = parse(pfaResult.toString).extract[A]
      val expected = parse(out).extract[A]
      (actual, expected) match {
        case (a: ScalerResult, e: ScalerResult) => assert(a.scaled === e.scaled)
        case (a: Result, e: Result) => assert(a === e)
      }
    }
  }

  def parityTest(
      sparkTransformer: Transformer,
      input: Array[String],
      expectedOutput: Array[String]): Unit = {
    val PFAJson = transformerToPFA(sparkTransformer, pretty = true)
    if (isDebug) {
      println(PFAJson)
    }
    val engine = getPFAEngine(PFAJson)
    testInputVsExpected(engine, input, expectedOutput)
  }
}

case class ScalerResult(scaled: Seq[Double]) extends Result

