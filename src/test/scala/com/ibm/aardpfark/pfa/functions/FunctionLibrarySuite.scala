package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.DSLSuiteBase
import org.json4s.DefaultFormats

trait FunctionLibrarySuite extends DSLSuiteBase {

  val tol = 0.001
  implicit val formats = DefaultFormats

}
