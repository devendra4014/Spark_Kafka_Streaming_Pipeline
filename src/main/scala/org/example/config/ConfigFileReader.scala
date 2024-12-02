package org.example.config

import com.google.gson.Gson
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedReader, InputStreamReader}

class ConfigFileReader {

  // Method to read JSON either from local or S3
  def readConfig(): Pipeline = {
    var dataPipe: Pipeline = null
    val gson = new Gson
    val configFilePath: String = AppConfig.configFile

    // If an S3 bucket and key are provided, try to read from S3 first
    if (configFilePath != null) {
      println(
        "[Read Config] Config File Path: " + configFilePath
      )

      if (configFilePath.startsWith("s3a://")) {
        if (AppConfig.accessKey
          .equalsIgnoreCase("") || AppConfig.secretKey
          .equalsIgnoreCase("") || AppConfig.region
          .equalsIgnoreCase("")) {
          println("accessKey and secretKey Not Provided")
        }
        else {
          val configFileString = ""
          dataPipe = gson.fromJson(configFileString, classOf[Pipeline])
        }
      }
      else {

        // Read from local or HDFS fileSystem
        val configuration: Configuration = new Configuration()
        val path: Path = new Path(configFilePath)
        val fileSystem: FileSystem = FileSystem.get(path.toUri, configuration)
        val bufferedReaderData = new BufferedReader(
          new InputStreamReader(fileSystem.open(path))
        )
        val configFileString = org.apache.commons.io.IOUtils.toString(bufferedReaderData)
        dataPipe = gson.fromJson(configFileString, classOf[Pipeline])
      }
    }
    else {
      println("Please provide configFile Path.")
    }
    dataPipe
  }
}

