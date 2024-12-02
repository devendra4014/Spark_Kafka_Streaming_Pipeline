package org.example.Inputs

import org.apache.spark.sql.DataFrame
import org.example.config.{AppConfig, Input}

class InputExecutor {

  def execute(inputArray: Array[Input]): Unit = {
    inputArray.foreach(input =>
      try {
        if (input.getInputType.equalsIgnoreCase("kafka")) {
          //call s3Input class
          val inputDataframe: DataFrame = new KafkaInput().executeKafkaInput(input)
          AppConfig.setDataFrame(input.getName, inputDataframe)
        } else if (
          input
            .getInputType
            .equalsIgnoreCase("local")
        ){

        }
       else {
          println("InputType is not supported")
        }
      }
      catch {
        case exception: Exception =>
          println(
            "[Read Input] [ERROR] Error in input Name: "
              .concat(input.getName)
              .concat(" with input Type: ")
              .concat(input.getInputType)
          )
          exception.printStackTrace()
          throw new RuntimeException(exception)
      }
    )
  }
}
