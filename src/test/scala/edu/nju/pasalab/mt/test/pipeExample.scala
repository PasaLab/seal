package edu.nju.pasalab.mt.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by YWJ on 2017.2.22.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object pipeExample {
  def main(args: Array[String]) {
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("pipe test").getOrCreate()

    val data = List("hi", "hello", "how", "are", "you")
    val dataRDD = spark.sparkContext.makeRDD(data)

    val scriptPath = "./script/echo.sh"
    //spark.sparkContext.addFile(scriptPath)

    val pipeRDD = dataRDD.pipe(scriptPath).collect()

    println(pipeRDD.mkString(", "))
    spark.stop()
  }
}
