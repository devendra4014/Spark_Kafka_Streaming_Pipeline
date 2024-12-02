package org.example.config

import kafka.cluster.EndPoint
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable


object AppConfig {
  var configFile: String = _
  var accessKey: String = _
  var secretKey: String = _
  var region: String = _
  var endpointsMap: Map[String, EndPoint] = _
  var inputMap: Map[String, Input] = _
  var dataframeMap: mutable.HashMap[String, DataFrame] = mutable.HashMap[String, DataFrame]()
  var sparkSession: SparkSession = _
  var kafkaCheckpointPath : String = _


  def setArgVar(args: Array[String]): Unit = {
    val argParser: ArgParser = new ArgParser(args)
    configFile = argParser.configFile()
    accessKey = argParser.accessKey()
    secretKey = argParser.secretKey()
    region = argParser.region()

  }

  def setEndPoints(endpoints: Array[EndPoint]) : Unit = {
    val endPointHashMap: mutable.HashMap[String, EndPoint] =
      new mutable.HashMap[String, EndPoint]

    endpoints.foreach{ x =>
      endPointHashMap.put(x.getName, x)
    }

    this.endpointsMap = endPointHashMap.toMap

  }

  def setDataFrame(dataFrameName: String, dataFrame: DataFrame): Unit = {
    dataframeMap.put(dataFrameName, dataFrame)
  }

  def getEndpoint(name: String): Map[String, String] = {
    // Get the EndPoint from the map (returns Option[EndPoint])
    this.endpointsMap.get(name)
    match {
      case Some(endPoint) => endPoint.getParam // Extract the EndPoint from Some and return its parameters
      case None => Map[String, String]() // Return an empty map if the key isn't found
    }
  }

  def getDataframe(name: String): DataFrame = {
    // Get the EndPoint from the map (returns Option[EndPoint])
    try {
      val dataFrame: DataFrame = this.dataframeMap(name)
      dataFrame
    } catch {
      case exception: Exception =>
       println(
          "[Dataframe Manager] [ERROR] Dataframe named: "
            .concat(name)
            .concat(" is not present. Please check the config file")
        )
        exception.printStackTrace()
        throw new RuntimeException()
    }
  }

  def getSparkSession: SparkSession = {
    if (sparkSession == null) {
      // Create SparkConf and set some default configurations
      val sparkConf = new SparkConf()
        .setAppName("Spark Framework - Starting at:" + System.currentTimeMillis())
//        .set("spark.extraListeners", "com.dataeaze.edp.driver.CustomListener")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
        //        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .setMaster("local") // Set the master URL (adjust the number of cores as needed)

      sparkSession = SparkSession
        .builder()
        .appName("Spark Framework -  Starting at:" + System.currentTimeMillis())
        .config(sparkConf)
        .getOrCreate()

    }
    sparkSession
  }

  def setSparkSession(spark: SparkSession): Unit = {
    sparkSession = spark
  }

  def setKafkaCheckpointpath(hdfspath: String): Unit = {
    this.kafkaCheckpointPath = hdfspath
  }

  def getKafkaCheckpointpath: String = {
    this.kafkaCheckpointPath
  }

}
