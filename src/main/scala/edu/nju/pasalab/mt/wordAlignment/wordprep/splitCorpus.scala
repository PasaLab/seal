package edu.nju.pasalab.mt.wordAlignment.wordprep

import java.io.{OutputStreamWriter, BufferedWriter, PrintWriter}
import edu.nju.pasalab.mt.util.{SntToCooc, CommonFileOperations}
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Created by YWJ on 2016/6/11.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object splitCorpus {
  val logger:Logger  = LoggerFactory.getLogger(splitCorpus.getClass)

  /**
    * @param mergedCorpus contains source and target , each line use " ||| " split
    * @param root : root directory on HDFS
    * @param coocDir : cooc directory on HDFS
    * @param ctrl :The control directory
    * @param memoryLimit : each partition memory max limit
    * @param sc : SparkContext
    */
  def execute(mergedCorpus : RDD[Array[String]], src : String, tgt : String, root : String, coocDir : String,
              ctrl : String, memoryLimit : Int, partNum :Int, sc : SparkSession): Int = {

    val pLimit = (memoryLimit * 1024L * 1024L) / 16

    val srcDict : Object2IntOpenHashMap[String] = new Object2IntOpenHashMap[String]
    for (elem <- sc.sparkContext.textFile(src).map(line => line.trim.split("\\s+")).collect()) {
      srcDict.put(elem(1), elem(0).toInt)
    }
    val tgtDict : Object2IntOpenHashMap[String] = new Object2IntOpenHashMap[String]
    for (elem <-  sc.sparkContext.textFile(tgt).map(line => line.trim.split("\\s+")).collect()) {
      tgtDict.put(elem(1), elem(0).toInt)
    }

    var smallDict : Boolean = false
    var currentSplit :Int = 0

    var srcSntFile : PrintWriter = null
    var tgtSntFile : PrintWriter = null

    var estimator : TypeTokenEstimator = new TypeTokenEstimator(400000)
    for (elem <- mergedCorpus.collect()) {
      if (!smallDict) {
        currentSplit += 1

        smallDict = true
        srcSntFile = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(CommonFileOperations.openFileForWrite(SntToCooc.getHDFSSrcCorpus(root, currentSplit), true)), 65 * 1024 * 1024))

        tgtSntFile =  new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(CommonFileOperations.openFileForWrite(SntToCooc.getHDFSTgtCorpus(root, currentSplit), true)), 65 * 1024 * 1024))
      }

      val srcWords = elem(0).trim.split("\\s+")
      val tgtWords = elem(1).trim.split("\\s+")
      val swids : Array[Int] = new Array[Int](srcWords.length)
      val twids : Array[Int] = new Array[Int](tgtWords.length)
      srcSntFile.print("1|")
      tgtSntFile.print("1|")
      for (i <- 0 until srcWords.length) {
        swids(i) = srcDict.getInt(srcWords(i))
        srcSntFile.print(swids(i))
        if (i == srcWords.length -1)
          srcSntFile.print("|")
        else
          srcSntFile.print(" ")
      }

      for (i <- 0 until tgtWords.length) {
        twids(i) = tgtDict.getInt(tgtWords(i))
        tgtSntFile.print(twids(i))
        srcSntFile.print(twids(i))
        if (i == tgtWords.length -1 ) {
          srcSntFile.print("\n")
          tgtSntFile.print("|")
        } else {
          srcSntFile.print(" ")
          tgtSntFile.print(" ")
        }
      }

      for (i <- 0 until swids.length) {
        tgtSntFile.print(swids(i))
        if (i == swids.length - 1)
          tgtSntFile.print("\n")
        else
          tgtSntFile.print(" ")
      }
      //
      if (!estimator.isLimitHit()) {
        for (a <- swids)
          for (b <- twids)
            estimator.newToken(a, b)
      } else
        estimator.justAddToken(swids.length * twids.length)

      val em : Long = estimator.getTypes.toLong
      if (em >= pLimit) {
        logger.info("Limit hit, current split " + currentSplit + " Estimated entries " + em)
        println("Limit hit, current split " + currentSplit + " Estimated entries " + em)
        srcSntFile.close()
        tgtSntFile.close()

        process(root, currentSplit, partNum, sc)
        smallDict = false
        estimator = new TypeTokenEstimator(400000)
      }
    }

    if (smallDict) {
      srcSntFile.close()
      tgtSntFile.close()

      logger.info("Finished reading , current split " + currentSplit + " Estimated entries " + estimator.getTypes.toLong)
      println("Finished reading , current split " + currentSplit + " Estimated entries " + estimator.getTypes.toLong)

      process(root, currentSplit, partNum, sc)
    }

    currentSplit
  }

  def process(root : String, split : Int, partNum : Int, sc : SparkSession): Unit = {
    val MAX_WORD : Long = 0x40000000L
    logger.info("source direction process\n")
    println("source direction process\n")
    val srcCorp = sc.sparkContext.textFile(SntToCooc.getHDFSSrcCorpus(root, split), partNum).filter(_.length > 0).map(line => line.trim.split("\\|"))
    val srcCooC = SntToCooc.getHDFSSrcCoocDir(root, split)
    snt2CooCProcess(srcCorp, srcCooC, MAX_WORD)

    logger.info("target direction process\n")
    println("target direction process\n")
    val tgtCorp = sc.sparkContext.textFile(SntToCooc.getHDFSTgtCorpus(root, split), partNum).filter(_.length > 0).map(line => line.trim.split("\\|"))
    val tgtCooc = SntToCooc.getHDFSTgtCoocDir(root, split)
    snt2CooCProcess(tgtCorp, tgtCooc, MAX_WORD)
  }

  def snt2CooCProcess(data : RDD[Array[String]], path : String, MAX_WORD : Long): Unit = {
    CommonFileOperations.deleteIfExists(path)
    logger.info("result save to " + path)
    println("result save to " + path)
    val co = data.flatMap(line => {
      val src = line(1).trim.split("\\s+")
      val tgt = line(2).trim.split("\\s+")
      val srcInt : Array[Int] = new Array[Int](src.length + 1)
      val tgtInt : Array[Int] = new Array[Int](tgt.length)
      var i = 1
      srcInt(0) = 0
      for (e <- src) {
        srcInt(i) = e.toInt
        i += 1
      }
      i = 0
      for (e <- tgt) {
        tgtInt(i) = e.toInt
        i += 1
      }
      val arr : ArrayBuffer[Long] = new ArrayBuffer[Long]
      for (a <- srcInt)
        for (b <- tgtInt) {
          arr.append(a * MAX_WORD + b)
        }
      arr.distinct
    }).distinct().map(elem => (elem / MAX_WORD, elem % MAX_WORD))
      .repartition(1).saveAsTextFile(path)

//    val oId: PrintWriter = new PrintWriter(new BufferedWriter
//    (new OutputStreamWriter(CommonFileOperations.openFileForWrite(path, true)), 65 * 1024 * 1024))
//
//    val list = co.collect().sortBy(elem => elem._1)
//    for (elem <- list)
//      oId.println(elem._1 + " " + elem._2)
//    oId.close()
  }

  def getSrcSntPathOnHDfS(root : String, split : Int) : String = {
    root + "/snt/src/" + String.format("%08d", new Integer(split)) + ".snt"
  }

  def getTgtSntPathOnHDfS(root : String, split : Int) : String = {
    root + "/snt/tar/" + String.format("%08d", new Integer(split)) + ".snt"
  }

  def getSrcCoocPathOnHDFS(root : String, split : Int) : String = {
    root + "/cooc/src/" +  String.format("%08d", new Integer(split)) + ".cooc"
  }

  def getTgtCoocPathOnHDFS(root : String, split : Int) : String = {
    root + "/cooc/tgt/" +  String.format("%08d", new Integer(split)) + ".cooc"
  }
}
