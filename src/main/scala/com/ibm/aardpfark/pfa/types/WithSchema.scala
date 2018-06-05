package com.ibm.aardpfark.pfa.types

import org.apache.avro.Schema

trait WithSchema {
  def schema: Schema
}
