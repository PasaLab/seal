package edu.nju.pasalab.mt.wordAlignment.usemgiza

import java.io.File
import java.util.concurrent.TimeUnit
import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.mt.wordAlignment.prodcls.ProduceClass
import edu.nju.pasalab.mt.wordAlignment.wordprep.wordAlignPrep
import edu.nju.pasalab.util.{ExtractionParameters, configRead}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by YWJ on 2016/7/8.
  ** Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */

object mainTraining{
  val logger = LoggerFactory.getLogger(mainTraining.getClass)
  def main(args: Array[String]) {

    val config = ConfigFactory.parseFile(new File(args(0)))

    val ep = configRead.getTrainingSettings(config)

    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer","128k")
      .config("spark.kryo.registrator", "edu.nju.pasalab.util.MyRegistrator")
      .config("spark.kryo.registrationRequired", ep.registrationRequired)
      .config("spark.default.parallelism", ep.parallelism)
      .config("spark.memory.offHeap.enabled",ep.isOffHeap)
      .config("spark.memory.offHeap.size", ep.offHeapSize)
      .config("spark.driver.extraJavaOptions", ep.driverExtraJavaOptions)
      .config("spark.executor.extraJavaOptions", ep.executorExtraJavaOptions)
      .getOrCreate()


    configRead.printTrainingSetting(ep)
    configRead.printIntoLog(ep)

    //totalPipeline(spark, ep)
    srcTraining(spark, ep)

    tgtTraining(spark, ep)

    mergeAlignment(spark, ep)

    spark.stop()
  }

  def srcTraining(spark : SparkSession, ep : ExtractionParameters): Unit = {
    val t1 = System.nanoTime()
    WordAlignMain.srcTraining(spark, ep)
    val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS) / 60000.0
    println("Use Spark Training Source Word Alignment use training sequence : " + ep.trainSeq + "\nTotal Build Time :" + T + " .min ")
  }

  def tgtTraining(spark : SparkSession, ep : ExtractionParameters): Unit = {
    val t1 = System.nanoTime()
    WordAlignMain.tgtTraining(spark, ep)
    val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS) / 60000.0
    println("Use Spark Training Source Word Alignment use training sequence : " + ep.trainSeq + "\nTotal Build Time :" + T + " .min ")
    spark.stop()
  }

  def mergeAlignment(spark : SparkSession, ep : ExtractionParameters): Unit = {
    val t1 = System.nanoTime()
    WordAlignMain.merge(spark, ep)
    val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS) / 60000.0
    println("Use Spark merge training sequence : " + ep.trainSeq + "\nTotal Build Time :" + T + " .min ")
  }

  def totalPipeline(spark : SparkSession, ep : ExtractionParameters): Unit = {

    logger.info("\nPrepare for Word Alignment\nStep 0-1: Start Make Class")
    println("\nPrepare for Word Alignment\nStep 0-1: Start Make Class")
    val pt1 = System.nanoTime()
    ProduceClass.execute(spark, ep)
    val pt2 = System.nanoTime()
    logger.info("\nMake Class have Done ! Waste time " + TimeUnit.MILLISECONDS.convert(pt2 - pt1, TimeUnit.NANOSECONDS) / 1000.0 + " .s \n")
    println("\nMake Class have Done ! Waste time " + TimeUnit.MILLISECONDS.convert(pt2 - pt1, TimeUnit.NANOSECONDS) / 1000.0 + " .s \n")

    logger.info("Prepare for Word Alignment\nStep 0-2: Generate Cooc File and Split merged Corpus.\n")
    println("Prepare for Word Alignment\nStep 0-2: Generate Cooc File and Split merged Corpus.\n")
    val wt1 = System.nanoTime()
    val splitNum = wordAlignPrep.execute(spark, ep)
    val wt2 = System.nanoTime()
    logger.info("Split Done! Waste time " + TimeUnit.MILLISECONDS.convert(wt2 - wt1, TimeUnit.NANOSECONDS) / 1000.0 + " .s \n")
    println("Split Done! Waste time " + TimeUnit.MILLISECONDS.convert(wt2 - wt1, TimeUnit.NANOSECONDS) / 1000.0 + " .s \n")

    println("split num :" + splitNum)


    logger.info("Now, Start Word Alignment\n")
    println("\nNow, Start Word Alignment\n")
    val t1 = System.nanoTime()
    WordAlignMain.execute(spark, ep)
    val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS) / 1000.0
    println("Use Spark Training Word Alignment use training sequence : " + ep.trainSeq + "\nTotal Build Time :" + T + " .s ")
  }

}
