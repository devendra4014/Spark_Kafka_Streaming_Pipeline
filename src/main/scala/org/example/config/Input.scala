package org.example.config

import org.apache.spark.sql.types._
import org.json4s
import org.json4s.{DefaultFormats, JValue, JsonAST, jackson, string2JsonInput}
import org.json4s.JsonAST.{JArray, JObject, JString}

class Input {
  private var dataframeName: String = _
  private var endPointName: String = _
  private var inputType : String = _
  private var params : Array[Parameter] = _
  private var schema: String = _


  def getEndPointName: String = endPointName
  def getName: String = dataframeName
  def getInputType : String = inputType
  def getParams : Map[String, String] = params.map(p => p.getKey() -> p.getvalue()).toMap
  def getSchema: StructType = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Parse the JSON string
    val jsonInput = string2JsonInput(schema)
    val parsedJson = jackson.parseJson(jsonInput)

    // Recursive method to convert the JSON structure into a Spark SQL StructType
    def convertJsonToStructType(json: JValue): DataType = {
      json match {
        case JObject(fields) =>
          // If the JSON object is a map (i.e., it's a StructType in Spark)
          val structFields = fields.map { case (name, value) =>
            StructField(name, convertJsonToStructType(value), nullable = true)
          }
          StructType(structFields)

        case JArray(items) =>
          // If the JSON is an array, it's an ArrayType in Spark
          ArrayType(convertJsonToStructType(items.head))

        case JString(s) if s == "string" => StringType
        case JString(s) if s == "long" => LongType
        case JString(s) if s == "double" => DoubleType
        case JString(s) if s == "integer" => IntegerType
        case _ => StringType // Default fallback to StringType for unknown cases
      }
    }

    // Convert the top-level parsed JSON to a StructType
    convertJsonToStructType(parsedJson).asInstanceOf[StructType]
  }

  private def setEndPointName (value: String) : Unit = {
    endPointName = value
  }

  private def setSchema(value: String): Unit = {
    this.schema = value
  }

  private def setInputType(value: String): Unit = {
    inputType = value
  }

  private def setParams(value: Array[Parameter]): Unit = {
    params = value
  }

  private def setInputName(name: String):Unit = {
    this.dataframeName = name
  }

  // Recursive method to convert the JSON structure into a Spark SQL StructType
  def convertJsonToStructType(json: JValue): DataType = {
    json match {
      case JObject(fields) =>
        // If the JSON object is a map (i.e., it's a StructType in Spark)
        val structFields = fields.map { case (name, value) =>
          StructField(name, convertJsonToStructType(value), nullable = true)
        }
        StructType(structFields)

      case JArray(items) =>
        // If the JSON is an array, it's an ArrayType in Spark
        ArrayType(convertJsonToStructType(items.head))

      case JString(s) if s == "string" => StringType
      case JString(s) if s == "long" => LongType
      case JString(s) if s == "double" => DoubleType
      case JString(s) if s == "integer" => IntegerType
      case _ => StringType // Default fallback to StringType for unknown cases
    }
  }




}
