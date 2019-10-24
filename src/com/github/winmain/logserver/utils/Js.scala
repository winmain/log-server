package com.github.winmain.logserver.utils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object Js {
  val mapper: ObjectMapper = newMapper

  def newMapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
}
