package org.example.TransformationBlock

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ColumnLevelFunctions {

  private def getDistinctValues(input: Column): Column = {
    array_distinct(split(input, " "))
  }

  val GetDistinctValuesFunction: FunctionTransformer = new FunctionTransformer {
    def apply(args: StringArgs[_]): Column =
      args match {
        case SingleArg(column, parameters) => getDistinctValues(column)
        case _ => throw new IllegalArgumentException("Invalid arguments")
      }

    def argType: String = "SingleColumn"
  }

}
