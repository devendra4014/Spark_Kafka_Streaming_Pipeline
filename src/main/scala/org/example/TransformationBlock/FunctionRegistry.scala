package org.example.TransformationBlock

import org.apache.spark.sql.Column
import org.example.config.Parameter


trait FunctionTransformer {
  def apply(args: StringArgs[_]): Column
  def argType: String
}

// Define the arguments that the functions will accept
sealed trait StringArgs[T] {
  val args: T
}

case class SingleArg(args: Column, parameters: Map[String, String]) extends StringArgs[Column]
case class TwoArg(args: (Column, Column), parameters: Map[String, String]) extends StringArgs[(Column, Column)]

object functionRegistry {

  val functions: Map[String, FunctionTransformer] = Map(
    "getDistinctValues" -> ColumnLevelFunctions.GetDistinctValuesFunction

  )

  def getFunction(name: String): Option[FunctionTransformer] = {
    functions.get(name).map(identity)
  }

}
