package org.example.store

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.example.config.{AppConfig, Output}

import scala.collection.mutable

class KafkaStore {

  private var topic: String = _
  private var kafkaServer: String = _
  private var checkPointDir: String = _
  private var outputMode: String = _
  private var key: String = "null"

  def getKafkaDataFrameProperties(output: Output): DataFrame = {
    val endPointMap: mutable.Map[String, String] = mutable.Map[String, String]()
    AppConfig.getEndpoint(output.getEndPointName).foreach(x => endPointMap.put(x._1, x._2))

    endPointMap.keys.foreach { key =>
      if (key == "kafkaServer") {
        this.kafkaServer = endPointMap(key)
      }
    }

    val parameters = output.getParams

    parameters.keys.foreach {
      key =>
        if (key == "topic") {
          this.topic = parameters(key)
        }
        if (key == "checkPointDir") {
          this.checkPointDir = parameters(key)
        }
        if (key == "outputMode"){
          this.outputMode = parameters(key)
        }
        if (key == "key") {
          this.key = parameters(key)
        }
    }
    AppConfig.getDataframe(output.getDataframeName)
  }

  def execute(store: Output) : Unit = {
    val dataframe : DataFrame = getKafkaDataFrameProperties(store)

    val dataToWrite = dataframe.selectExpr(
      s"CAST($key AS STRING) AS key",
      "to_json(struct(*)) AS value"
    )


    val kafkaWriteQuery = dataToWrite.writeStream
      .queryName("kafka Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", this.kafkaServer)
      .option("topic", this.topic)
      .outputMode(this.outputMode)
      .option("checkpointLocation", this.checkPointDir)
      .start()

    //  logger.info("Listening and writing to Kafka")
    kafkaWriteQuery.awaitTermination()

  }
}
