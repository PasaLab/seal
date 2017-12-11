package edu.nju.pasalab.mt.wordAlignment.wordprep

import java.io.IOException
import edu.nju.pasalab.mt.util.CommonFileOperations
import edu.nju.pasalab.mt.wordAlignment.programLogic
import edu.nju.pasalab.util.ExtractionParameters
import programLogic._
import org.apache.commons.logging.{LogFactory, Log}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by YWJ on 2016/6/11.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object wordAlignPrep {
  val logger:Logger  = LoggerFactory.getLogger(wordAlignPrep.getClass)
  val LOG : Log = LogFactory.getLog(wordAlignPrep.getClass)

  var inputSource : String = ""
  var inputTarget : String = ""
  var distHDFS : String = ""


  def execute(sc : SparkSession, ep : ExtractionParameters): Int = {


    inputSource = ep.alignedRoot + "/src-raw-corpus"
    inputTarget = ep.alignedRoot + "/tgt-raw-corpus"

    logger.info("Word Alignment Prop Start\n")
    // 1. merged Corpus
    val mergedCorpus: String = ep.alignedRoot + "/merged-corpus" // 1. Merged corpus

    // 2. Dictionaries
    val sourceDict: String = ep.alignedRoot + "/dict/src"
    val targetDict: String = ep.alignedRoot + "/dict/tgt"

    // 2.1 Temporary dict Files
    val sourceDictTemp: String = ep.alignedRoot + "/dict/srctemp"
    val targetDictTemp: String = ep.alignedRoot + "/dict/tgttemp"

    // 3. SNT file dir
    val sntDir: String = ep.alignedRoot + "/snt/"

    val coocDir: String = ep.alignedRoot + "/cooc/"

    // 4. Ctrl file
    val ctrl: String = ep.alignedRoot + "/ctrl"

    // 5. vcb.classes files
    val srcClassHDFS: String = ep.alignedRoot + "/dict/src.classes"
    val tgtClassHDFS: String = ep.alignedRoot + "/dict/tgt.classes"

    //step 1
    if (ep.redo) {
      init(mergedCorpus, sourceDict, targetDict, sntDir, coocDir, ctrl, ep.srcClass, ep.tgtClass,
        srcClassHDFS, tgtClassHDFS, sourceDictTemp, targetDictTemp, ep)

      println("Step 1. Merging Corpus Onto HDFS\n")
      logger.info("Step 1. Merging Corpus Onto HDFS\n")
      if (!ep.nocopy)  {
        merged.execute(inputSource, inputTarget, mergedCorpus, ep.maxSentLen, ep.encoding)
      }
      if (ep.srcClass.length > 0 && ep.tgtClass.length > 0) {
        logger.info("Copying vocabuary classes to HDFS " + ep.srcClass + " ==> " + srcClassHDFS)
        CommonFileOperations.copyToHDFS(ep.srcClass, srcClassHDFS)
        logger.info("Copying vocabuary classes to HDFS " + ep.tgtClass + " ==> " + tgtClassHDFS)
        CommonFileOperations.copyToHDFS(ep.tgtClass, tgtClassHDFS)
      }
    }

    val data = sc.sparkContext.textFile(mergedCorpus, ep.parallelism).filter(_.length > 0).map(line => line.trim.split("\\|\\|\\|", 2))
    //data.persist(StorageLevel.MEMORY_ONLY)

    logger.info("\nStep 2. Building Dictionary and Sort by frequency")
    println("\nStep 2. Building Dictionary and Sort by frequency")
    val dict = buildDictionary.execute(data, sourceDict, targetDict, sc)
//    for (elem <- dict)
//      elem.persist(StorageLevel.MEMORY_ONLY)

    logger.info("\nStep 3. Generating splits")
    println("\nStep 3. Generating splits")

    val splitNum = splitCorpus.execute(data, sourceDict, targetDict, ep.alignedRoot, coocDir, ctrl, ep.memoryLimit, ep.parallelism, sc)
    println("\nFinal Corpus split number is: " + splitNum)
    logger.info("\nFinal Corpus split number is: " + splitNum)
   // data.unpersist()
//    for (elem <- dict)
//      elem.unpersist()

    splitNum
  }

  def init(mergedCorpus : String, sourceDict : String, targetDict : String, sntDir : String, coocDir : String, ctrl : String,
           srcClass : String, tgtClass: String, srcClassHDFS : String, tgtClassHDFS : String, sourceDictTemp : String, targetDictTemp : String, ep : ExtractionParameters): Unit = {
    val fs : FileSystem = FileSystem.get(new Configuration())
    if (!ep.overwrite) {
      if (ep.nocopy)
        testFileExistOnHDFSOrDie(mergedCorpus, "File does not exists: " + mergedCorpus + "On HDFS", new IOException().getClass, LOG)
      else
        testFileNotExistOnHDFSOrDie(mergedCorpus , "File Already exists " + mergedCorpus + " On HDFS", new IOException().getClass, LOG)
      testFileNotExistOnHDFSOrDie(sourceDict, "File already exists : " + sourceDict + " on HDFS ", new IOException().getClass, LOG)
      testFileNotExistOnHDFSOrDie(targetDict, "File already exists : " + targetDict + " on HDFS ", new IOException().getClass, LOG)
      testFileNotExistOnHDFSOrDie(sntDir, "File already exists : " + sntDir + " on HDFS ", new IOException().getClass, LOG)
      testFileNotExistOnHDFSOrDie(coocDir, "File already exists : " + coocDir + " on HDFS ", new IOException().getClass, LOG)
      testFileNotExistOnHDFSOrDie(ctrl, "File already exists : " + ctrl + " on HDFS ", new IOException().getClass, LOG)

      if (srcClass.length > 0 && tgtClass.length > 0) {
        testFileNotExistOnHDFSOrDie(tgtClassHDFS, "File already exists : " + tgtClassHDFS + " on HDFS ", new IOException().getClass, LOG)
        testFileNotExistOnHDFSOrDie(srcClassHDFS, "File already exists : " + srcClassHDFS + " on HDFS ", new IOException().getClass, LOG)
      }
    } else {
      if (ep.nocopy)
        testFileExistOnHDFSOrDie(mergedCorpus, "File does not exist : " + mergedCorpus + " on HDFS ", new IOException().getClass, LOG)
      else
        deleteIfExists(mergedCorpus, fs)
      deleteIfExists(sourceDict, fs)
      deleteIfExists(targetDict, fs)
      deleteIfExists(sntDir, fs)
      deleteIfExists(coocDir, fs)
      deleteIfExists(ctrl, fs)
      if (srcClass.length > 0 && tgtClass.length > 0) {
        deleteIfExists(srcClassHDFS, fs)
        deleteIfExists(tgtClassHDFS, fs)
      }
    }

    if (srcClass.length > 0 && tgtClass.length > 0) {
      testFileExistOrDie(tgtClass, "File does not exists " + tgtClass, new IOException().getClass, LOG)
      testFileExistOrDie(srcClass, "File does not exists " + srcClass, new IOException().getClass, LOG)
    } else {
      testFileExistOnHDFSOrDie(tgtClassHDFS, "File does not exists " + tgtClassHDFS + " specify tgtcclass or generate by mkcls", new IOException().getClass, LOG)
      testFileExistOnHDFSOrDie(srcClassHDFS, "File does not exists " + srcClassHDFS + " specify srcclass or generate by mkcls", new IOException().getClass, LOG)
    }
    deleteIfExists(sourceDictTemp, fs)
    deleteIfExists(targetDictTemp, fs)
  }

  def  deleteIfExists(fileName : String, fs : FileSystem): Unit = {
    val path = new Path(fileName)
    if (fs.exists(path))
      if (!fs.delete(path, true)) {
        System.err.println("can not delete file " + fileName)
        logger.info("can not delete file " + fileName, throw new IOException())
      }
  }
}
