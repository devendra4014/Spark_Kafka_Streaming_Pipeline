package org.example.config

trait Transformer {
  def transformationType: String
  def functionName: String
}

case class ColumnTransformer(
  transformationType: String,
  functionName: String,
  inputColumn: Array[String],
  outputColumn: String,
  params: Array[Parameter],

) extends Transformer

case class Transformations(
                            inputDataFrame: Array[String],
                            retainColumns: Array[String],
                            outputDataFrame: String,
                            transformers: Array[ColumnTransformer]
)

