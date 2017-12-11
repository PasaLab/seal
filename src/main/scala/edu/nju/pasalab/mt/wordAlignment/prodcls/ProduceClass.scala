package edu.nju.pasalab.mt.wordAlignment.prodcls

import java.util.concurrent.TimeUnit

import edu.nju.pasalab.mt.util.SntToCooc
import edu.nju.pasalab.util.ExtractionParameters
import org.apache.commons.logging.{LogFactory, Log}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by YWJ on 2016/6/6.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object ProduceClass {

  val logger = LoggerFactory.getLogger(ProduceClass.getClass)
  val LOG : Log = LogFactory.getLog(ProduceClass.getClass)
  def execute(sc : SparkSession, ep : ExtractionParameters): Unit = {

    if (ep.nocopy)
      if (ep.source.trim.length == 0 || ep.target.trim.length == 0)
        logger.info("In case --nocopy is not specified, you need to give me the pointer to source/target corpus files",
          throw new Exception)

    val src : Boolean = true
    val tgt : Boolean = false

    val srcInputCorpusHDFS: String = ep.alignedRoot + "/src-raw-corpus"
    val tgtInputCorpusHDFS: String = ep.alignedRoot + "/tgt-raw-corpus"

    val srcWorkingDir: String = ep.alignedRoot + "/mkcls/src/temp"
    val tgtWorkingDir: String = ep.alignedRoot + "/mkcls/tgt/temp"

    val srcWordID: String = ep.alignedRoot + "/mkcls/src.idmap"
    val tgtWordID: String = ep.alignedRoot + "/mkcls/tgt.idmap"

    val srcDigitCorpus: String = ep.alignedRoot + "/mkcls/src/dcorpus"
    val tgtDigitCorpus: String = ep.alignedRoot + "/mkcls/tgt/dcorpus"

    val srcOutput: String = ep.alignedRoot + "/dict/src.classes"
    val tgtOutput: String = ep.alignedRoot + "/dict/tgt.classes"

    if (ep.redo) {
      logger.info("start File detect")
      val t1 = System.nanoTime()
      FileDetection.execute(ep.nocopy, ep.overwrite, ep.source, srcInputCorpusHDFS, ep.target, tgtInputCorpusHDFS,
        srcOutput, tgtOutput, ep.maxSentLen, ep.filterLog, LOG)
      val t2 = System.nanoTime()
      val T = TimeUnit.MILLISECONDS.convert(t2 - t1, TimeUnit.NANOSECONDS)
      System.out.println("File detect Done, waste " + T + " ms .")
      logger.info("File detect Done, waste " + T + " ms .\n" + "Running initialization")
    }

    logger.info("1-1.\tSource Direction Initialization.buildLexicon ")
    System.out.println("Running initialization" + "\n1-1.\tSource Direction Initialization.buildLexicon")
    val srcCorpusEncode = Initialization.buildLexicon(srcInputCorpusHDFS, srcWorkingDir, srcWordID,
      SntToCooc.getClassMapDir(ep.alignedRoot, src, 0), srcDigitCorpus, ep.numCls, ep.parallelism, sc)
    srcCorpusEncode.persist()

    //src Class Map --> id classID count
    //                  Int int int
    val srcClsMap = sc.sparkContext.textFile(SntToCooc.getClassMapDir(ep.alignedRoot, src, 0), ep.parallelism).persist()

    logger.info("1-2.\tSource Direction BigramCount.build Bigram")
    System.out.println("\n1-2.\tSource Direction BigramCount.build Bigram")
    //src class Bigram
    val srcClsBigram = BigramCount.buildBigram(srcCorpusEncode, SntToCooc.getBigramDir(ep.alignedRoot, src, 1),
      SntToCooc.getClsBigramDir(ep.alignedRoot, src, 1), srcClsMap, ep.numCls, sc)

    srcClsBigram.persist()
    logger.info("1-3\tSource Direction Likelihood.getLikelihood")
    System.out.println("\n1-3\tSource Direction Likelihood.getLikelihood")
    Likelihood.getLikelihood(srcClsBigram, srcClsMap,
      SntToCooc.getLikelihoodFile(ep.alignedRoot, src, 0),
      srcWorkingDir, sc)
    srcClsMap.unpersist()

    //target Direction
    logger.info("2-1\tTarget Direction Initialization.buildLexicon")
    System.out.println("\n2-1\ttarget Direction Initialization.buildLexicon")
    val tgtCorpusEncode = Initialization.buildLexicon(tgtInputCorpusHDFS, tgtWorkingDir, tgtWordID,
      SntToCooc.getClassMapDir(ep.alignedRoot, tgt, 0), tgtDigitCorpus, ep.numCls, ep.parallelism, sc)
    tgtCorpusEncode.persist()

    //tgt Class Map
    val tgtClsMap = sc.sparkContext.textFile(SntToCooc.getClassMapDir(ep.alignedRoot, tgt, 0), ep.parallelism).persist()
    //src class Bigram
    logger.info("2-2\tTarget Direction BigramCount.buildBigram")
    System.out.println("\n2-2\ttarget Direction BigramCount.buildBigram")
    val tgtClsBigram = BigramCount.buildBigram(tgtCorpusEncode, SntToCooc.getBigramDir(ep.alignedRoot, tgt, 1),
      SntToCooc.getClsBigramDir(ep.alignedRoot, tgt, 1),
      tgtClsMap, ep.numCls, sc)
    tgtClsBigram.persist()

    logger.info("2-3\tTarget Direction Likelihood.getLikelihood")
    System.out.println("\n2-3\ttarget Direction Likelihood.getLikelihood")
    Likelihood.getLikelihood(tgtClsBigram, tgtClsMap,
      SntToCooc.getLikelihoodFile(ep.alignedRoot, tgt , 0),
      tgtWorkingDir, sc)
    tgtClsMap.unpersist()
    //Iterative
    logger.info("start Iteration " + ep.iterations)
    System.out.println("\nstart Iteration " + ep.iterations)

    var srcPrevClsBigram : Option[RDD[(Long, Long)]] = Some(srcClsBigram)
    var tgtPrevClsBigram : Option[RDD[(Long, Long)]] = Some(tgtClsBigram)
    for (i <- 1 until ep.iterations + 1) {
      val curSrcBigram = sc.sparkContext.textFile(SntToCooc.getBigramDir(ep.alignedRoot, src, i), ep.parallelism)
      //src class Map
      logger.info(i + 2 + "-1\tSource Direction UpdateClass.updateClass " + i)
      System.out.println(i + 2 + "-1\tSource Direction UpdateClass.updateClass " + i )
      val curSrcClsMap = UpdateClass.updateClass(curSrcBigram, srcPrevClsBigram.get,
        SntToCooc.getClassMapDir(ep.alignedRoot, src, i), ep.numCls, sc)
      srcPrevClsBigram.get.unpersist()
      curSrcClsMap.persist()

      logger.info(i + 2 + "-2\tSource Direction BigramCount.buildBigram " + i)
      System.out.println(i + 2 + "-2\tSource Direction BigramCount.buildBigram " + i)
      val curSrcClsBigram = BigramCount.buildBigram(srcCorpusEncode, SntToCooc.getBigramDir(ep.alignedRoot, src, i+1),
        SntToCooc.getClsBigramDir(ep.alignedRoot, src, i+1), curSrcClsMap, ep.numCls, sc)

      logger.info(i + 2 + "-3\tSource Direction Likelihood.getLikelihood " + i)
      System.out.println(i + 2 + "-3\tSource Direction Likelihood.getLikelihood " + i)
      Likelihood.getLikelihood(curSrcClsBigram, curSrcClsMap, SntToCooc.getLikelihoodFile(ep.alignedRoot, src, i), srcWorkingDir, sc)
      curSrcClsMap.unpersist()
      srcPrevClsBigram = Some(curSrcClsBigram)

      //target direction
      val tgtSrcBigram = sc.sparkContext.textFile(SntToCooc.getBigramDir(ep.alignedRoot, tgt, i))
      //src class Map
      logger.info(i + 2 + "-1\tTarget Direction  UpdateClass.updateClass " + i)
      System.out.println(i + 2 + "-1\tTarget Direction  UpdateClass.updateClass " + i)
      val curTgtClsMap = UpdateClass.updateClass(tgtSrcBigram, tgtPrevClsBigram.get,
        SntToCooc.getClassMapDir(ep.alignedRoot, tgt, i), ep.numCls, sc)

      tgtPrevClsBigram.get.unpersist()
      curTgtClsMap.persist()
      logger.info(i + 2 + "-2\tTarget Direction BigramCount.buildBigram " + i)
      System.out.println(i + 2 + "-2\tTarget Direction BigramCount.buildBigram " + i)
      val curTgtClsBigram = BigramCount.buildBigram(tgtCorpusEncode, SntToCooc.getBigramDir(ep.alignedRoot, tgt, i+1),
        SntToCooc.getClsBigramDir(ep.alignedRoot, tgt, i+1), curTgtClsMap, ep.numCls, sc)

      logger.info(i + 2 + "-3\tTarget Direction Likelihood.getLikelihood " + i)
      System.out.println(i + 2 + "-3\tTarget Direction Likelihood.getLikelihood " + i)
      Likelihood.getLikelihood(curTgtClsBigram, curTgtClsMap, SntToCooc.getLikelihoodFile(ep.alignedRoot, tgt, i), tgtWorkingDir, sc)
      curTgtClsMap.unpersist()
      tgtPrevClsBigram = Some(curTgtClsBigram)
    }
    srcPrevClsBigram.get.unpersist()
    tgtPrevClsBigram.get.unpersist()

    logger.info("Outputing final result")
    System.out.println("Outputing final result")
    Initialization.mapClassBackToWord(SntToCooc.getClassMapDir(ep.alignedRoot, src, ep.iterations), srcWordID, srcOutput, ep.parallelism, sc)

    Initialization.mapClassBackToWord(SntToCooc.getClassMapDir(ep.alignedRoot, tgt, ep.iterations), tgtWordID, tgtOutput, ep.parallelism, sc)

    logger.info("Removing RDD from persistence list")
    System.out.println("Removing RDD from persistence list")
    srcCorpusEncode.unpersist()
    tgtCorpusEncode.unpersist()
  }

 /* def getLikelihoodFile(rootDir: String, src: Boolean, iter: Int): String = {
    String.format("%s/mkcls/%s/it%03d/likelihood", rootDir, if (src) "src" else "tgt", new Integer(iter))
  }

  def getClassMapDir(rootDir: String, src: Boolean, iter: Int): String = {
    String.format("%s/mkcls/%s/it%03d/classMap", rootDir, if (src) "src" else "tgt", new Integer(iter))
  }

  def getBigramDir(rootDir: String, src: Boolean, iter: Int): String = {
    String.format("%s/mkcls/%s/it%03d/bigram", rootDir, if (src) "src" else "tgt", new Integer(iter))
  }

  def getClsBigramDir(rootDir: String, src: Boolean, iter: Int): String = {
    String.format("%s/mkcls/%s/it%03d/classbigram", rootDir, if (src) "src" else "tgt", new Integer(iter))
  }*/
}
