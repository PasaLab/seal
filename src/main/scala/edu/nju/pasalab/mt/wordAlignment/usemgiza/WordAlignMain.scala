package edu.nju.pasalab.mt.wordAlignment.usemgiza

import java.io.{IOException, FileNotFoundException}
import java.util
import java.util.concurrent.TimeUnit
import chaski.utils.{CommandSheet, BinaryToStringCodec}
import edu.nju.pasalab.mt.util.{SntToCooc, CommonFileOperations}
import edu.nju.pasalab.mt.wordAlignment.util.{GIZAAlignmentTask, ExternalUtils}
import edu.nju.pasalab.util.ExtractionParameters

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
/**
  * Created by YWJ on 2016/6/8.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object WordAlignMain {

  val logger = LoggerFactory.getLogger(WordAlignMain.getClass)
  var gizaParas : util.Map[String, String] = null

  def execute(sc : SparkSession, ep : ExtractionParameters) {
    init(ep.alignedRoot)

    GIZAAlignmentTask.setD4normBinary(ep.d4norm)
    GIZAAlignmentTask.setGizaBinary(ep.giza)
    GIZAAlignmentTask.setHmmnormBinaray(ep.hmmnorm)
    val paras: util.Map[String, String] = new util.TreeMap[String, String]

    if (gizaParas != null) {
      for(ent <- paras.entrySet()) {
        val key = ent.getKey.substring(5).toLowerCase()
        paras.put(key, ent.getValue)
      }
    }
    logger.info("Start Word Alignment Training, Source to Target .")
    println("Start Word Alignment Training, Source to Target .\n")
    val tSrc1 = System.nanoTime()
    val SrcSeq: TrainingSequence = new TrainingSequence(ep.trainSeq, true, ep.alignedRoot, ep.memoryLimit)
    SrcSeq.setGizaParas(paras)

    SrcSeq.runTraining(ep.totalReducer, ep.splitNum, ep.nCPUs, sc)
    val tSrc = TimeUnit.MILLISECONDS.convert(System.nanoTime() - tSrc1, TimeUnit.NANOSECONDS) / 1000.0
    logger.info("Source to Target Word Alignment Training waste time " + tSrc + " .s \n")
    println("Source to Target Word Alignment Training waste time " + tSrc + " .s \n")


    //target direction
    logger.info("Start Word Alignment Training, Target to Source .")
    println("Start Word Alignment Training, Target to Source .\n")
    val tTgt1 = System.nanoTime()
    val tgtSeq : TrainingSequence = new TrainingSequence(ep.trainSeq, false, ep.alignedRoot, ep.memoryLimit)
    tgtSeq.setGizaParas(paras)

    tgtSeq.runTraining(ep.totalReducer, ep.splitNum, ep.nCPUs, sc)
    val tTgt = TimeUnit.MILLISECONDS.convert(System.nanoTime() - tTgt1, TimeUnit.NANOSECONDS) / 1000.0
    logger.info("Target to Source Word Alignment Training waste time " + tTgt + " .s \n")
    println("Target to Source Word Alignment Training waste time " + tTgt + " .s \n")


    //merge stage
    logger.info("Start merge alignment result, last step\n")
    println("Start merge alignment result, last step\n")
    val mergeOutput = ep.alignedRoot + "/mergeResult"
    GIZAAlignmentTask.setSymalbin(ep.symalbin)
    GIZAAlignmentTask.setSymalmethod(ep.symalmethod)
    GIZAAlignmentTask.setSymalscript(ep.symalscript)
    GIZAAlignmentTask.setGiza2bal(ep.giza2bal)
    val mT1 = System.nanoTime()
    val taskList = GIZAAlignmentTask.buildMerging(ep.alignedRoot, mergeOutput, ep.splitNum)

    sc.sparkContext.parallelize(taskList, taskList.size())
      .mapPartitionsWithIndex((j, iter) => iter.map(t => (j,t)))
      .partitionBy(new HashPartitioner(taskList.size()))
      .foreachPartition(part => {
      val codec = new BinaryToStringCodec(false)
        part.foreach(t => {
          val entrySplit = t._2.split("\t")
          val key = entrySplit(0)
          val value = entrySplit(1)
          //var test = key
          val cmd: CommandSheet = codec.decodeObject(value).asInstanceOf[CommandSheet]
          val message: util.LinkedList[String] = new util.LinkedList[String]
          val isSuccess: Boolean = ExternalUtils.runCommand(cmd, key, message)
          if (isSuccess) {
            logger.info("****************************SUCCESS**************************")
          } else {
            val bf =new StringBuilder(64)
            for (elem <- message)
              bf.append(elem).append("********* #NL# *****************")
            logger.info(bf.toString())
            throw new IOException(bf.toString())
          }
      })
    })

    val mT = TimeUnit.MILLISECONDS.convert(System.nanoTime() - mT1, TimeUnit.NANOSECONDS) / 1000.0

    println("Last Step merge, total waste time " + mT + " .s\n")
  }

  def srcTraining(sc : SparkSession, ep : ExtractionParameters) {
    init(ep.alignedRoot)

    GIZAAlignmentTask.setD4normBinary(ep.d4norm)
    GIZAAlignmentTask.setGizaBinary(ep.giza)
    GIZAAlignmentTask.setHmmnormBinaray(ep.hmmnorm)
    val paras: util.Map[String, String] = new util.TreeMap[String, String]

    if (gizaParas != null) {
      for(ent <- paras.entrySet()) {
        val key = ent.getKey.substring(5).toLowerCase()
        paras.put(key, ent.getValue)
      }
    }

    println("Start Word Alignment Training, Source to Target .\n")
    val tSrc1 = System.nanoTime()
    val SrcSeq: TrainingSequence = new TrainingSequence(ep.trainSeq, true, ep.alignedRoot, ep.memoryLimit)
    SrcSeq.setGizaParas(paras)

    SrcSeq.runTraining(ep.totalReducer, ep.splitNum, ep.nCPUs, sc)

    val tSrc = TimeUnit.MILLISECONDS.convert(System.nanoTime() - tSrc1, TimeUnit.NANOSECONDS) / 60000.0
    println("Source to Target Word Alignment Training waste time " + tSrc + " .min \n")

  }


  def tgtTraining(sc : SparkSession, ep : ExtractionParameters) {

    CommonFileOperations.deleteIfExists(ep.alignedRoot +  "/training/T2S")
    GIZAAlignmentTask.setD4normBinary(ep.d4norm)
    GIZAAlignmentTask.setGizaBinary(ep.giza)
    GIZAAlignmentTask.setHmmnormBinaray(ep.hmmnorm)
    val paras: util.Map[String, String] = new util.TreeMap[String, String]

    if (gizaParas != null) {
      for(ent <- paras.entrySet()) {
        val key = ent.getKey.substring(5).toLowerCase()
        paras.put(key, ent.getValue)
      }
    }

    logger.info("Start Word Alignment Training, Target to Source .")
    println("Start Word Alignment Training, Target to Source .\n")
    val tTgt1 = System.nanoTime()
    val tgtSeq : TrainingSequence = new TrainingSequence(ep.trainSeq, false, ep.alignedRoot, ep.memoryLimit)
    tgtSeq.setGizaParas(paras)

    tgtSeq.runTraining(ep.totalReducer, ep.splitNum, ep.nCPUs, sc)
    val tTgt = TimeUnit.MILLISECONDS.convert(System.nanoTime() - tTgt1, TimeUnit.NANOSECONDS) / 60000.0

    println("Target to Source Word Alignment Training waste time " + tTgt + " .min \n")

  }

  def merge(sc:SparkSession, ep : ExtractionParameters) {

    val mergeOutput = ep.alignedRoot + "/mergeResult"
    CommonFileOperations.deleteIfExists(mergeOutput)
    GIZAAlignmentTask.setD4normBinary(ep.d4norm)
    GIZAAlignmentTask.setGizaBinary(ep.giza)
    GIZAAlignmentTask.setHmmnormBinaray(ep.hmmnorm)
    val paras: util.Map[String, String] = new util.TreeMap[String, String]

    if (gizaParas != null) {
      for(ent <- paras.entrySet()) {
        val key = ent.getKey.substring(5).toLowerCase()
        paras.put(key, ent.getValue)
      }
    }


    println("Start merge alignment result, last step\n")
    GIZAAlignmentTask.setSymalbin(ep.symalbin)
    GIZAAlignmentTask.setSymalmethod(ep.symalmethod)
    GIZAAlignmentTask.setSymalscript(ep.symalscript)
    GIZAAlignmentTask.setGiza2bal(ep.giza2bal)
    val mT1 = System.nanoTime()
    val taskList = GIZAAlignmentTask.buildMerging(ep.alignedRoot, mergeOutput, ep.splitNum)


    sc.sparkContext.parallelize(taskList, taskList.size())
      .mapPartitionsWithIndex((j, iter) => iter.map(t => (j, t)))
      .partitionBy(new HashPartitioner(taskList.size()))
      .foreachPartition(part => {
        val codec = new BinaryToStringCodec(false)
        part.foreach(t => {
          val entrySplit = t._2.split("\t")
          val key = entrySplit(0)
          val value = entrySplit(1)
          val cmd: CommandSheet = codec.decodeObject(value).asInstanceOf[CommandSheet]
          val message: util.LinkedList[String] = new util.LinkedList[String]
          val isSuccess: Boolean = ExternalUtils.runCommand(cmd, key, message)
          if (isSuccess) {
            logger.info("****************************SUCCESS**************************")
          } else {
            val bf =new StringBuilder(64)
            for (elem <- message)
              bf.append(elem).append("********* #NL# *****************")
            logger.info(bf.toString())
            throw new IOException(bf.toString())
          }
        })
      })

    val mT = TimeUnit.MILLISECONDS.convert(System.nanoTime() - mT1, TimeUnit.NANOSECONDS) / 1000.0

    println("Last Step merge, total waste time " + mT + " .s\n")
  }

  def init(rootDir : String): Unit = {
    val sourceDict: String = SntToCooc.getHDFSSrcVCBDir(rootDir)
    val targetDict: String = SntToCooc.getHDFSTgtVCBDir(rootDir)

    val sntDir: String = SntToCooc.getHDFSCorpusDir(rootDir)
    val coocDir: String = SntToCooc.getHDFSCoocDir(rootDir)

    val tgtClassHDFS: String = SntToCooc.getHDFSTgtVCBClassPath(rootDir)
    val srcClassHDFS: String = SntToCooc.getHDFSSrcVCBClassPath(rootDir)

    val conf : Configuration = new Configuration()
    val hdfsFile = new Path(sourceDict)
    conf.addResource(new Path("bin/core-site.xml"))
    conf.addResource(new Path("bin/hdfs-site.xml"))

    val fs :FileSystem = hdfsFile.getFileSystem(conf)
    if (!fs.exists(new Path(sourceDict))) {
      logger.info(sourceDict + " is Not On HDFS ", throw new FileNotFoundException(sourceDict + " is Not On HDFS "))
    }
    if (!fs.exists(new Path(targetDict))) {
      logger.info(targetDict + " is Not On HDFS ", throw new FileNotFoundException(targetDict + " is Not On HDFS "))
    }
    if (!fs.exists(new Path(sntDir))) {
      logger.info(sntDir + " is Not On HDFS ", throw new FileNotFoundException(sntDir + " is Not On HDFS "))
    }
    if (!fs.exists(new Path(coocDir))) {
      logger.info(coocDir + " is Not On HDFS ", throw new FileNotFoundException(coocDir + " is Not On HDFS "))
    }
    if (!fs.exists(new Path(srcClassHDFS))) {
      logger.info(srcClassHDFS + " is Not On HDFS ", throw new FileNotFoundException(srcClassHDFS + " is Not On HDFS "))
    }
    if (!fs.exists(new Path(tgtClassHDFS))) {
      logger.info(tgtClassHDFS + " is Not On HDFS ", throw new FileNotFoundException(tgtClassHDFS + " is Not On HDFS "))
    }

    CommonFileOperations.deleteIfExists(rootDir + "/training")
    logger.info(rootDir + "/training")
  }
}
