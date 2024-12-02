package org.example.config

class Pipeline {
  private var projectName : String = _
  private var endPoints : Array[EndPoint] = _
  private var inputs : Array[Input] = _
  private var transformations : Array[Transformations] = _
  private var output : Array[Output] = _

  def getProjectName : String = projectName
  def getEndpoints : Array[EndPoint] = endPoints
  def getInputs : Array[Input] = inputs
  def getTransformations : Array[Transformations] = transformations
  def getOutputs : Array[Output] = output

  private  def setProjectName(value: String): Unit = {
    this.projectName = value
  }

  private def setEndpoints(value: Array[EndPoint]): Unit = {
    this.endPoints = value
  }

  private def setInputs(value: Array[Input]): Unit = {
    this.inputs = value
  }
  private def setTransformations(value: Array[Transformations]): Unit = {
    this.transformations = value
  }
    private def setOutputs(value: Array[Output]): Unit = {
    this.output = value
  }
}

