package com.ibm.aardpfark.pfa.expression

import org.json4s.JValue


trait PFAExpression {
  def json: JValue
}