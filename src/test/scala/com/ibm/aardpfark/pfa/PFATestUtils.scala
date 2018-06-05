package com.ibm.aardpfark.pfa

import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

trait PFATestUtils {

  def getPFAEngine(json: String, debug: Boolean = false) = {
    PFAEngine.fromJson(json, multiplicity = 1, debug = debug).head
  }

}

object PFATestUtils extends PFATestUtils