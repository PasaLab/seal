package edu.nju.pasalab.mt.extraction.spark.exec

import java.io.File
import java.util.concurrent.TimeUnit
import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.mt.extraction.preProccess
import edu.nju.pasalab.mt.util.CommonFileOperations
import edu.nju.pasalab.util.{ExtractType, configRead}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object MainExtraction {
  val logger  = LoggerFactory.getLogger(MainExtraction.getClass)
  def main(args: Array[String]) {

    val config = ConfigFactory.parseFile(new File(args(0)))

    val ep = configRead.getTrainingSettings(config)
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer","128k")
      .config("spark.kryo.registrationRequired", ep.registrationRequired)
      .config("spark.kryo.registrator", "edu.nju.pasalab.util.MyRegistrator")
      .config("spark.memory.offHeap.enabled",ep.isOffHeap)
      .config("spark.memory.offHeap.size", ep.offHeapSize)
      .config("spark.default.parallelism", ep.parallelism)
      .config("spark.driver.extraJavaOptions", ep.driverExtraJavaOptions)
      .config("spark.executor.extraJavaOptions", ep.executorExtraJavaOptions)
      //.config("Direction", "C2E")
      .getOrCreate()

    //set compress type
    //org.apache.hadoop.io.compress.BZip2Codec
    //org.apache.hadoop.io.compress.GzipCodec
    //org.apache.hadoop.io.compress.Lz4Codec
    //org.apache.hadoop.io.compress.SnappyCodec
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "true")
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.codec", ep.compressType)
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    //Print user program configuration
    configRead.printTrainingSetting(ep)
    configRead.printIntoLog(ep)
    CommonFileOperations.deleteIfExists(ep.rootDir)

    println("Pre process corpus")
    val t0 = System.nanoTime()
    preProccess.execute(spark, ep)

    val T0 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t0, TimeUnit.NANOSECONDS) / 1000.0
    println("\n***************Preprocess corpus waste " + T0 + " .s *******************************\n")

    if(ep.optType == ExtractType.Phrase) {
      //logger.info("Phrase Translation Model Build")
      println("Phrase Translation Model Build")
      val t = System.nanoTime()

      PhraseBasedModel.computeLexicalPro(spark, ep)

      val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t, TimeUnit.NANOSECONDS) / 60000.0
      logger.info("\n***************Total waste " + T + " .minutes *******************************\n")
      println("\n***************Total waste " + T + " .minutes *******************************\n")

    } else if (ep.optType == ExtractType.Rule) {
      println("Hiero-Phrase Translation Model Build")
      val t = System.nanoTime()

      HieroPhraseBasedModel.computeLexicalPro(spark, ep)

      val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t, TimeUnit.NANOSECONDS) / 60000.0
      logger.info("\n***************Total waste " + T + " .minutes *******************************\n")
      println("\n***************Total waste " + T + " .minutes *******************************\n")
    }
    spark.stop()
  }
}
