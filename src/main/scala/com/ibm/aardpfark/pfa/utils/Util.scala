package com.ibm.aardpfark.pfa.utils

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object Utils {
  def getCurrentDate = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"))
    fmt.format(new Date())
  }

  def getCurrentTs = {
    new Date().getTime
  }
}
