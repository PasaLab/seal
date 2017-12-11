import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.mt.extraction.preProccess
import edu.nju.pasalab.mt.extraction.spark.exec.{HieroPhraseBasedModel, PhraseBasedModel}
import edu.nju.pasalab.mt.util.CommonFileOperations
import edu.nju.pasalab.util.{ExtractType, configRead}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

/**
  * Created by YWJ on 2016.12.25.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object test {
  def main(args: Array[String]) {
   // val logger  = LoggerFactory.getLogger(test.getClass)
    System.setProperty("hadoop.home.dir", "D:\\workspace\\winutils")

    val configFile = "data/training.conf"
    val config = ConfigFactory.parseFile(new File(configFile))

    val ep = configRead.getTrainingSettings(config)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer","128k")
      .config("spark.kryo.registrator", "edu.nju.pasalab.util.MyRegistrator")
      .config("spark.kryo.registrationRequired", ep.registrationRequired)
      .config("spark.default.parallelism", ep.parallelism)
      .config("spark.memory.offHeap.enabled",ep.isOffHeap)
      .config("spark.memory.offHeap.size", ep.offHeapSize)
      .config("spark.driver.extraJavaOptions", ep.driverExtraJavaOptions)
      .config("spark.executor.extraJavaOptions", ep.executorExtraJavaOptions)
      .config("Direction", "C2E")
      .getOrCreate()

    //org.apache.hadoop.io.compress.BZip2Codec
    //org.apache.hadoop.io.compress.GzipCodec
    //org.apache.hadoop.io.compress.Lz4Codec
    //org.apache.hadoop.io.compress.SnappyCodec
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "true")
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.codec", ep.compressType)
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    configRead.printTrainingSetting(ep)
    configRead.printIntoLog(ep)
    CommonFileOperations.deleteIfExists(ep.rootDir)
    val t0 = System.nanoTime()
    preProccess.execute(spark, ep)


    if(ep.optType == ExtractType.Phrase){
      println("Phrase Translation Model Build")
      val t = System.nanoTime()
      PhraseBasedModel.computeLexicalPro(spark, ep)

      val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t, TimeUnit.NANOSECONDS) / 60000.0
      val T0 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t0, TimeUnit.NANOSECONDS) / 60000.0
      println("\n***************Total waste " + T + " .minutes *******************************\n")

      println("\n***************include dict, Total waste " + T0 + " .minutes *******************************\n")
    }
    else if (ep.optType == ExtractType.Rule)  {
      println("Phrase Translation Model Build")
      val t = System.nanoTime()
      HieroPhraseBasedModel.computeLexicalPro(spark , ep)
      val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t, TimeUnit.NANOSECONDS) / 60000.0

      val T0 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t0, TimeUnit.NANOSECONDS) / 60000.0

      println("\n***************Total waste " + T + " .minutes *******************************\n")
      println("\n***************include dict, Total waste " + T0 + " .minutes *******************************\n")
    }
    spark.stop()
  }
}
