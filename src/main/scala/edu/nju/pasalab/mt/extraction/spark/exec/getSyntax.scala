package edu.nju.pasalab.mt.extraction.spark.exec

import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.mt.extraction.preProccess
import edu.nju.pasalab.mt.util.CommonFileOperations
import edu.nju.pasalab.util.configRead
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by YWJ on 2016.12.27.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object getSyntax {
  val logger  = LoggerFactory.getLogger(getSyntax.getClass)
  def main(args: Array[String]) {

    //    Logger.getLogger("org").setLevel(Level.OFF)
    //    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer", "128k")
      //.config("spark.kryo.registrator", "edu.nju.pasalab.util.MyRegistrator")
      .config("Direction", "C2E")
      .getOrCreate()


    logger.info("list parameters \n")
    println("list parameters \n")
    for (elem <- args) {
      logger.info("--- > " + elem)
      println("--- > " + elem)
    }
    /**
      * args(0) config file
      */
    val config = ConfigFactory.parseFile(new File(args(0)))

    val ep = configRead.getTrainingSettings(config)

    configRead.printTrainingSetting(ep)
    configRead.printIntoLog(ep)
    CommonFileOperations.deleteIfExists(ep.rootDir)


    logger.info("preprocess corpus")
    println("preprocess corpus")
    val t0 = System.nanoTime()
    preProccess.fun(spark, ep)
    val T0 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t0, TimeUnit.NANOSECONDS) / 1000.0
    logger.info("\n***************preprocess corpus waste " + T0 + " .s *******************************\n")
    println("\n***************preprocess corpus waste " + T0 + " .s *******************************\n")
    spark.stop()
  }
}
