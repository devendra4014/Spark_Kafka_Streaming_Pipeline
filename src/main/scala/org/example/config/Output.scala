package org.example.config

class Output {

  private var endPointName: String = _
  private var outputDFName: String = _
  private var outputType: String = _
  private var params: Array[Parameter] = _


  def getEndPointName: String = endPointName

  def getOutputType: String = outputType

  def getDataframeName: String = outputDFName

  def getParams: Map[String, String] = params.map(p => p.getKey() -> p.getvalue()).toMap

  private def setEndPointName(value: String): Unit = {
    endPointName = value
  }

  private def setDataframeName(value: String): Unit = {
    this.outputDFName = value
  }

  private def setInputType(value: String): Unit = {
    outputType = value
  }

  private def setParams(value: Array[Parameter]): Unit = {
    params = value
  }

}
