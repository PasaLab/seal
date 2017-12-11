package edu.nju.pasalab.mt.extraction.spark.dataSet

import java.util.concurrent.TimeUnit

import edu.nju.pasalab.mt.util.SntToCooc
import edu.nju.pasalab.util.ExtractionParameters
import org.apache.spark.sql.SparkSession
import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by YWJ on 2016.12.31.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object PhraseBaseDataSet {
  val logger:Logger  = LoggerFactory.getLogger(PhraseBaseDataSet.getClass)
  def execute(sc: SparkSession, ep:ExtractionParameters): Unit = {
    logger.info("\nCompute Word translation probability table\n")
    println("\nCompute Word translation probability table\n")
    val t1 = System.nanoTime()

    val probMap = sc.sparkContext.broadcast(wordTranslationProb.getWordMap(
      wordTranslationProb.countWordPairs(sc, SntToCooc.getHDFSTMPath(ep.rootDir), ep.partitionNum), sc))

    val T1 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS) / 1000.0
    logger.info("\nCompute Word translation probability table waste " + T1 + " .s\n")
    println("\nCompute Word translation probability table waste " + T1 + " .s\n")


    logger.info("Extract Phrase")
    println("Extract Phrase")
    val t2 = System.nanoTime()
  }
}
