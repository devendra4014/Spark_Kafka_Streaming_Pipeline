package org.example.TransformationBlock

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.functions.{col, expr}
import org.example.config.{AppConfig, ColumnTransformer, Parameter, Transformations}


class TransformPipeline {

  def getParameters(params: Array[Parameter]): Map[String, String] = {
    val parameters: Map[String, String] =
    if (params == null || params.isEmpty) {
        Map.empty[String, String]
      } else {
        params.map (param => param.getKey () -> param.getvalue () ).toMap
      }
    parameters
  }

  def createArgs(argType: String, columnValues: Array[String], parameters: Array[Parameter]): StringArgs[_] = {
    argType match {
      case "SingleColumn" =>
        if (columnValues.length == 1) {
          val args = SingleArg(col(columnValues.head), getParameters(parameters))
          args
        } else {
          throw new IllegalArgumentException("SingleColumn requires Single columns.")
        }

      case "DoubleColumn" =>
        if (columnValues.length == 2) {
          val cols = (col(columnValues(1)), col(columnValues(2)))
          val args = TwoArg(cols, getParameters(parameters))
          args
        } else {
          throw new IllegalArgumentException("DoubleColumn requires two columns.")
        }
      case _ => throw new IllegalArgumentException(s"Unsupported argument type: $argType")
    }

  }

  def executeTransformation(transformations: Array[Transformations]) : Unit = {

    transformations.foreach{ transformation =>
      val df = AppConfig.getDataframe(transformation.inputDataFrame.head)
      val retainedDF = df.select(transformation.retainColumns.map(col):_*)

      val finalDataframe = transformation.transformers.foldLeft(retainedDF){
        (accDF, transformer) =>
          transformer match {
            case columnTransformer: ColumnTransformer =>
              val inputColumn = columnTransformer.inputColumn.head
              val outputColumn = columnTransformer.outputColumn
              functionRegistry.getFunction(columnTransformer.functionName)
               match {
                case Some(function) =>
                  val functionArgs: StringArgs[_] = createArgs(function.argType, transformer.inputColumn, transformer.params)
                  val transformedColumn = function(functionArgs)
                  accDF.withColumn(transformer.outputColumn, transformedColumn)

                case _ => df
              }

            case _ =>  throw new IllegalArgumentException("Function not found.")
          }

      }

      AppConfig.setDataFrame(transformation.outputDataFrame, finalDataframe)
    }
  }

}
