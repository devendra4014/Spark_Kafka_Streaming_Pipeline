package org.example.config

import org.rogach.scallop.{ScallopConf, ScallopOption}



class ArgParser(arguments: Seq[String]) extends ScallopConf(arguments) {
  val configFile: ScallopOption[String] = opt[String]("configFile", default = Some(""))
  val accessKey: ScallopOption[String] =
    opt[String]("accessKey", default = Some(""))
  val secretKey: ScallopOption[String] =
    opt[String]("secretKey", default = Some(""))
  val region: ScallopOption[String] =
    opt[String]("region", default = Some(""))
  verify()
}

