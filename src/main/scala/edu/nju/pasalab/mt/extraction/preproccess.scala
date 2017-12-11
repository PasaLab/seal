package edu.nju.pasalab.mt.extraction

import java.io.{OutputStreamWriter, BufferedWriter, PrintWriter}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import edu.nju.pasalab.mt.extraction.dataStructure.SpecialString
import edu.nju.pasalab.mt.extraction.util.parser.Sentence
import edu.nju.pasalab.mt.util.{CommonFileOperations, SntToCooc}
import edu.nju.pasalab.util.{SyntaxType, ExtractionParameters, tupleEncoders}
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by YWJ on 2016.12.25.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object preProccess extends tupleEncoders {

  val empty:Pattern = Pattern.compile("^\\s*$")
  val logger :Logger  = LoggerFactory.getLogger(preProccess.getClass)

  /**
    * This
    * @param sc : Spark Context
    * @param ep Program run parameter
    */
  def execute(sc: SparkSession, ep:ExtractionParameters): Unit = {

    val part : Int = ep.parallelism / 2
    lazy val data = sc.sparkContext.textFile(ep.inputFileName, part).filter(_.length >= 0).map(line => line.split("\t"))

    val t1 = System.nanoTime()
    println("Build Dict")
    val src = data.map(line => line(0)).flatMap(line => line.trim.split("\\s+").map(elem => (elem , 1)))
      .reduceByKey(_+_)

    val tgt = data.map(line => line(1)).flatMap(line => line.trim.split("\\s+").map(elem => (elem , 1)))
      .reduceByKey(_+_)

    var hdfsDictFile : PrintWriter = new PrintWriter(new BufferedWriter(
      new OutputStreamWriter(CommonFileOperations.openFileForWrite(SntToCooc.getCorpusSrcDict(ep.rootDir), true)), 129 * 1024 * 1024))
    var id = 2
    val srcDict : Object2IntOpenHashMap[String] = new Object2IntOpenHashMap()
    srcDict.put("#", 0)
    srcDict.put("UNK", 1)
    hdfsDictFile.println("# " + 0)
    hdfsDictFile.println("UNK " + 1)
    src.collect().sortWith(_._2 > _._2).foreach(elem => {
      srcDict.put(elem._1, id)
      hdfsDictFile.println(elem._1 + " " + id)
      id += 1
    })
    hdfsDictFile.close()

    hdfsDictFile = new PrintWriter(new BufferedWriter(
      new OutputStreamWriter(CommonFileOperations.openFileForWrite(SntToCooc.getCorpusTgtDict(ep.rootDir), true)), 129 * 1024 * 1024))
    id = 2
    val tgtDict : Object2IntOpenHashMap[String] = new Object2IntOpenHashMap()
    hdfsDictFile.println("# " + 0)
    hdfsDictFile.println("UNK " + 1)
    tgtDict.put("#", 0)
    tgtDict.put("UNK", 1)
    tgt.collect().sortWith(_._2 > _._2).foreach(elem => {
      tgtDict.put(elem._1, id)
      hdfsDictFile.println(elem._1 + " " + id)
      id += 1
    })
    hdfsDictFile.close()

    val T1 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS) / 1000.0
    logger.info("Build Dict cost " + T1 + " s.")
    println("Build Dict cost " + T1 + " s.")

    val scrDictBroadcast = sc.sparkContext.broadcast(srcDict)
    val tgtDictBroadcast = sc.sparkContext.broadcast(tgtDict)
    val epBroadcast = sc.sparkContext.broadcast(ep)

    data.mapPartitions(part => {
      val srcDict_ = scrDictBroadcast.value
      val tgtDict_ = tgtDictBroadcast.value
      val eps = epBroadcast.value
      part.map(elem => {
        val sp = new StringBuilder(256)
        sp.append(digital(elem(0), srcDict_)).append(SpecialString.mainSeparator)
          .append(digital(elem(1), tgtDict_)).append(SpecialString.mainSeparator)
          .append(elem(2))
        if (eps.syntax == SyntaxType.S2D && eps.withSyntaxInfo && elem.size > 3) {
          sp.append(SpecialString.mainSeparator).append(elem(3))
            .append(SpecialString.mainSeparator).append(elem(4))
        }
        sp.toString()
      })
    }).saveAsTextFile(SntToCooc.getHDFSTMPath(ep.rootDir))
  }

  def dataSetExecute(sc: SparkSession, ep:ExtractionParameters): Unit = {

    lazy val data = sc.read.textFile(ep.inputFileName).filter(_.length > 0).map(line => line.split("\t")).repartition(ep.partitionNum)


    val t1 = System.nanoTime()
    logger.info("Build Dict")
    println("Build Dict")

    lazy val src = data.map(line => line(0)).flatMap(line => line.trim.split("\\s+")).groupByKey(x => x).mapGroups((k, v) => (k, v.length))


    lazy val tgt = data.map(line => line(1)).flatMap(line => line.trim.split("\\s+")).groupByKey(x => x).mapGroups((k, v) => (k, v.length))


    var hdfsDictFile : PrintWriter = new PrintWriter(new BufferedWriter(
      new OutputStreamWriter(CommonFileOperations.openFileForWrite(SntToCooc.getCorpusSrcDict(ep.rootDir), true)), 65 * 1024 * 1024))
    var id = 2
    val srcDict : Object2IntOpenHashMap[String] = new Object2IntOpenHashMap()
    srcDict.put("#", 0)
    srcDict.put("UNK", 1)
    hdfsDictFile.println("# " + 0)
    hdfsDictFile.println("UNK " + 1)
    src.collect().sortWith(_._2 > _._2).foreach(elem => {
      srcDict.put(elem._1, id)
      hdfsDictFile.println(elem._1 + " " + id)
      id += 1
    })
    hdfsDictFile.close()

    hdfsDictFile = new PrintWriter(new BufferedWriter(
      new OutputStreamWriter(CommonFileOperations.openFileForWrite(SntToCooc.getCorpusTgtDict(ep.rootDir), true)), 65 * 1024 * 1024))
    id = 2
    val tgtDict : Object2IntOpenHashMap[String] = new Object2IntOpenHashMap()
    hdfsDictFile.println("# " + 0)
    hdfsDictFile.println("UNK " + 1)
    tgtDict.put("#", 0)
    tgtDict.put("UNK", 1)
    tgt.collect().sortWith(_._2 > _._2).foreach(elem => {
      tgtDict.put(elem._1, id)
      hdfsDictFile.println(elem._1 + " " + id)
      id += 1
    })
    hdfsDictFile.close()

    val T1 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS) / 1000.0
    logger.info("Build Dict cost " + T1 + " s.")
    println("Build Dict cost " + T1 + " s.")

    val scrDictBroadcast = sc.sparkContext.broadcast(srcDict)
    val tgtDictBroadcast = sc.sparkContext.broadcast(tgtDict)
    val epBroadcast = sc.sparkContext.broadcast(ep)


    val xx = data.mapPartitions(part => {
      val srcDict_ = scrDictBroadcast.value
      val tgtDict_ = tgtDictBroadcast.value
      val eps = epBroadcast.value
      part.map(elem => {
        val sp = new StringBuilder(256)
        sp.append(digital(elem(0), srcDict_)).append(SpecialString.mainSeparator)
          .append(digital(elem(1), tgtDict_)).append(SpecialString.mainSeparator)
          .append(elem(2))
        if (eps.syntax == SyntaxType.S2D && eps.withSyntaxInfo && elem.size > 3) {

          sp.append(SpecialString.mainSeparator).append(elem(3))
            .append(SpecialString.mainSeparator).append(elem(4))
        }
        sp.toString()
      })
    }).rdd
      xx.saveAsTextFile(SntToCooc.getHDFSTMPath(ep.rootDir))
  }

  def digital(elem : String, dict : Object2IntOpenHashMap[String]) : String = {
    val sp = new StringBuilder(128)
    val split = elem.trim.split("\\s+")
    sp.append(dict.getInt(split(0)))
    for (i <- 1 until split.length) {
      sp.append(" ").append(dict.getInt(split(i)))
    }
    sp.toString()
  }
  def getPos(se : Sentence): String = {
    val sp = new StringBuilder(128)
    val pos = se.pos

    if (pos(0) == "-LRB-") sp.append("(")
    else if (pos(0) == "-RRB-") sp.append(")")
    else sp.append(pos(0))

    for (i <- 1 until pos.length) {
      if (pos(i) == "-LRB-") sp.append(" (")
      else if (pos(i) == "-RRB-") sp.append(" )")
      else sp.append(" ").append(pos(i))
    }
    sp.toString()
  }

  def getDependencyIndex(se : Sentence): String = {
    val sp = new StringBuilder(128)
    val head = se.stanfordDependencies
    sp.append(head(0)._1)

    for(i <- 1 until head.length)
      sp.append(" ").append(head(i)._1)
    sp.toString()
  }

  def fun(sc: SparkSession, ep:ExtractionParameters): Unit = {

    val da = sc.read.textFile(ep.inputFileName).repartition(ep.partitionNum * 2).filter(_.length > 0).cache()
    da.show(1)
    da.mapPartitions(part => {
      part.map(elem => {
        val split = elem.split("\t")
        val sp = new StringBuilder(256)
        sp.append(split(0)).append(SpecialString.mainSeparator)
          .append(split(1)).append(SpecialString.mainSeparator)
          .append(split(2))
        val se = Sentence(split(1))
        sp.append(SpecialString.mainSeparator).append(getDependencyIndex(se))
          .append(SpecialString.mainSeparator).append(getPos(se))
        sp.toString()
      })
    }).rdd.saveAsTextFile(ep.rootDir)
    da.unpersist()


   /* val data = sc.sparkContext.textFile(ep.inputFileName, ep.partitionNum * 2).filter(_.length > 0).persist(StorageLevel.MEMORY_ONLY)
    logger.info("**************************************** \n" + data.count() + "\n")
    data.mapPartitions(part => {
      part.map(elem => {
        val split = elem.split("\t")
        val sp = new StringBuilder
        sp.append(split(0)).append(SpecialString.mainSeparator)
          .append(split(1)).append(SpecialString.mainSeparator)
          .append(split(2))
        val se = Sentence(split(1))
        sp.append(SpecialString.mainSeparator).append(getDependencyIndex(se))
            .append(SpecialString.mainSeparator).append(getPos(se))
        sp.toString()
      })
    }).saveAsTextFile(ep.rootDir)
    data.unpersist()*/
  }
}
