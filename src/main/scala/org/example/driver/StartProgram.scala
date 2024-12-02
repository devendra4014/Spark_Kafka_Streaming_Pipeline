package org.example.driver

import org.example.Inputs.InputExecutor
import org.example.TransformationBlock.TransformPipeline
import org.example.config.{AppConfig, ConfigFileReader, EndPoint, Input, Output, Pipeline, Transformations}
import org.example.store.StoreExecutor

object StartProgram {

  def main(args: Array[String]): Unit = {
    // set commandline arguments
    AppConfig.setArgVar(args)

    val reader: ConfigFileReader = new ConfigFileReader()
    val data: Pipeline = reader.readConfig()

    val endpointArray: Array[EndPoint] = data.getEndpoints
    val inputArray: Array[Input] = data.getInputs
    val transformationsArray: Array[Transformations] = data.getTransformations
    val outputArray: Array[Output] = data.getOutputs

    AppConfig.setEndPoints(endpointArray)

    val inputExecutor: InputExecutor = new InputExecutor
    inputExecutor.execute(inputArray)

    if (transformationsArray != null) {
      if (transformationsArray.length > 0) {
        val pipelineTransform: TransformPipeline = new TransformPipeline()
        pipelineTransform.executeTransformation(transformationsArray)
      }
    }

    val storeExecutor: StoreExecutor = new StoreExecutor
    storeExecutor.executor(outputArray)


  }

}
