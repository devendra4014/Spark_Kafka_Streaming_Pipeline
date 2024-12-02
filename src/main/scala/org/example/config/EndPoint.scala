package org.example.config

import scala.collection

class EndPoint {
  private var endPointType: String = _
  private var name: String = _
  private var param: Array[Parameter] = _
  private var paramType: String = _

  def getEndPointType: String = {
    endPointType
  }

  def getName: String = {
    name
  }

  def getParam: Map[String,String] = {
    param.map(p => p.getKey() -> p.getvalue()).toMap
  }

  def getParamType: String = {
    paramType
  }

  def setEndPointType(endPointType: String): Unit = {
    this.endPointType = endPointType
  }

  def setName(name: String): Unit = {
    this.name = name
  }

  def setParam(param: Array[Parameter]): Unit = {
    this.param = param
  }

  def setParamType(paramType: String): Unit = {
    this.paramType = paramType
  }

}
