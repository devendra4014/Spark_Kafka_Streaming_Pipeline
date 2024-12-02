package org.example.Inputs

import kafka.utils.Json
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.example.config.{AppConfig, Input}
import org.zalando.spark.jsonschema.SchemaConverter

import scala.::
import scala.collection.mutable
import scala.io.Source

class KafkaInput {
  private var topic : String = _
  private var kafkaServer : String = _
  private var offSet : String = _
  private var schema : StructType = _
  private var format : String = _
  private var schemaRegistryUrl: String = _

  def getKafkaDataFrameProperties(input: Input) : String = {
    val endPointMap: mutable.Map[String, String] = mutable.Map[String, String]()
      AppConfig.getEndpoint(input.getEndPointName).foreach(x => endPointMap.put(x._1, x._2))

    endPointMap.keys.foreach { key =>
      if (key == "kafkaServer") {
        this.kafkaServer = endPointMap(key)
      }
    }

    val parameters = input.getParams

    parameters.keys.foreach{
      key =>
        if (key == "topic") {
          this.topic = parameters(key)
        }
        if (key == "offSet") {
          this.offSet = parameters(key)
        }
        if (key == "format") {
          this.format = parameters(key)
        }
        if (key == "registryUrl"){
          this.schemaRegistryUrl = parameters(key)
        }
    }

    if (this.format == "json") {
      val httpClient = HttpClients.createDefault()
      val httpGet = new HttpGet(this.schemaRegistryUrl)

      // Execute the request and retrieve the schema
      val response = httpClient.execute(httpGet)
      val responseString = EntityUtils.toString(response.getEntity)
      this.schema = SchemaConverter.convertContent(responseString)
    }

    var columnList = List[StructField]()

    this.schema = input.getSchema
    input.getName
  }

  private def loadDfFromKafka(dataframeName : String) : DataFrame = {
    val df = AppConfig.getSparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", this.kafkaServer)
      .option("subscribe", this.topic)
      .option("startingOffsets", this.offSet)
      .load()

    if (this.format != null){
      if (this.format == "json" && this.schema != null ) {
        val jsonDF = df.select(from_json(col("value").cast("string"), schema).alias("value"))
          .select("value.*")

        jsonDF
      }
        else if (this.format == "avro" && this.schemaRegistryUrl != null){
        val httpClient = HttpClients.createDefault()
        val httpGet = new HttpGet(this.schemaRegistryUrl)

        // Execute the request and retrieve the schema
        val response = httpClient.execute(httpGet)
        val avroSchema = EntityUtils.toString(response.getEntity)
        httpClient.close()

        // Now you can use the schema directly in from_avro function
        val avroDF = df.select("key", "value")
          .withColumn("value", from_avro(col("value"), avroSchema))
          .select("key", "value.*")

        avroDF
      }
      else df
    }
    else {
      df
    }

  }

  def executeKafkaInput(input: Input): DataFrame = {
    //val getOptions: GetOptions = new GetOptions()

    val inputDfName: String = getKafkaDataFrameProperties(input)
    val inputdataframe = loadDfFromKafka(inputDfName)
    Console.println("*********************************************************inputdataframe**************************", inputdataframe)
    inputdataframe
  }
}
