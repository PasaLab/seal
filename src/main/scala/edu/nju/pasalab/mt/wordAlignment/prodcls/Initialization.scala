package edu.nju.pasalab.mt.wordAlignment.prodcls

import java.io._
import java.util
import java.util.Collections
import edu.nju.pasalab.mt.util.CommonFileOperations
import edu.nju.pasalab.mt.wordAlignment.util.SortEntry
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap
import it.unimi.dsi.fastutil.objects.Object2LongAVLTreeMap
import org.apache.spark.sql.SparkSession
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD

/**
  * Created by YWJ on 2016/6/6.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object Initialization {

  val logger:Logger  = LoggerFactory.getLogger(Initialization.getClass)

  def buildLexicon(inputCorpus :String, workingRoot: String, outputId :String, outputMap : String,
                   outputCorpus : String, cls : Int, parallelism : Int, sc : SparkSession): RDD[String] ={
   val tempFile = workingRoot + "/lexicon-temp"


    val wordCount = sc.sparkContext.textFile(inputCorpus, parallelism).flatMap(line => {
      val sp = line.trim().split("\\s+").map(elem => (elem, 1L))
      sp
    }).reduceByKey(_+_)

    //CommonFileOperations.deleteIfExists(tempFile)
    //wordCount.map(x => x._1 + " " + x._2).saveAsTextFile(tempFile)

    SortAnsAssignClass(wordCount, outputMap, cls, outputId, tempFile, sc)

    //wordCount.unpersist()

    val wordID = sc.sparkContext.textFile(outputId).filter(_.length > 0).collect()

    val AVLMap : Object2LongAVLTreeMap[String] = new Object2LongAVLTreeMap[String]()
    for (elem <- wordID) {
      val split = elem.trim.split("\\s+")
      AVLMap.put(split(0), split(1).toLong)
    }

    val idBroadCast = sc.sparkContext.broadcast(AVLMap)
    val encode = sc.sparkContext.textFile(inputCorpus, parallelism).mapPartitions(part => {
      val id = idBroadCast.value
      part.map(line => {
        val splits = line.trim.split("\\s+")
        val sb = new StringBuilder(128)
        sb.append(id.getLong(splits(0)))
        for (i <- 1 until splits.length) {
          sb.append(" ").append(id.getLong(splits(i)))
        }
        sb.toString().trim
      })
    })
    logger.info("\tDigital Corpus save to " + outputCorpus)
    System.out.println("\tDigital Corpus save to " + outputCorpus)
    CommonFileOperations.deleteIfExists(outputCorpus)
    encode.saveAsTextFile(outputCorpus)
    encode
  }

  def SortAnsAssignClass(wordCount : RDD[(String, Long)] , outputMap: String,
                         cls : Int, outputId : String, temp : String , sc : SparkSession): Unit = {

    val idMap = wordCount.map(elem => (elem._2, elem._1)).repartition(1).sortByKey(false).collect()
    val list : util.LinkedList[SortEntry] = new util.LinkedList[SortEntry]()
    for (elem <- idMap) {
      list.add(new SortEntry(elem._1, elem._2))
    }
    Collections.sort(list)
//    val inp: HDFSDirInputStream = new HDFSDirInputStream(input)
//    val rd: BufferedReader = new BufferedReader(new InputStreamReader(inp))
//    var line: String = null
//    val list : util.LinkedList[SortEntry] = new util.LinkedList[SortEntry]()
//    line = rd.readLine()
//    while (line != null && line.length > 0) {
//      val sp = line.trim.split("\\s+")
//      if (sp.length != 2)
//        throw new IOException("Invalid Entry :" + line)
//      else {
//        val count = sp(1).toLong
//        list.add(new SortEntry(count, sp(0)))
//      }
//      line = rd.readLine()
//    }
//    Collections.sort(list)
//    rd.close()


    CommonFileOperations.deleteIfExists(outputId)
    CommonFileOperations.deleteIfExists(outputMap)

    val oId: PrintWriter = new PrintWriter(new BufferedWriter
    (new OutputStreamWriter(CommonFileOperations.openFileForWrite(outputId, true)), 65 * 1024 * 1024))

    val oMap: PrintWriter = new PrintWriter(new BufferedWriter
    (new OutputStreamWriter(CommonFileOperations.openFileForWrite(outputMap, true)), 65 * 1024 * 1024))

    var classId = 1
    var id = 2
    //(wordId || classId || wordCount )
    for (elem <- list) {
      oId.println(elem.word + "\t" + id)
      val sb = new StringBuilder(128)
      sb.append(id).append("\t").append(classId).append(" ").append(elem.count)
      oMap.println(sb.toString())
      id += 1
      if (classId < cls) classId += 1
    }
    oId.close()
    oMap.close()
//    val map : Array[String] = new Array[String](idMap.length)
//    //id classID count
//    for (i <- 0 until idMap.length) {
//      val sb = new StringBuilder
//      sb.append(tmp(i)._2).append("\t").append(classId).append(" ").append(tmp(i)._1)
//      map(i) = sb.toString()
//      if (classId < cls) classId += 1
//    }
//
//    logInfo("\tClass Map save to " + outputMap)
//    System.out.println("\tClass Map save to " + outputMap)
//    CommonFileOperations.deleteIfExists(outputMap)
//    sc.makeRDD(map).saveAsTextFile(outputMap)
//
//    //key id
//    idMap.map(x => (x._1, x._3))
  }

  def mapClassBackToWord(classMap : String, idMap : String, output : String, partNum :Int, sc : SparkSession): Unit = {


    //TODO use Join operation

    logger.info("process idMap")
    System.out.println("process idMap")

    val ids: java.util.LinkedList[String] = new util.LinkedList[String]
    ids.add("UNK") // id 0
    ids.add("<s>") // id 1

//    var inp: InputStream = CommonFileOperations.openFileForRead(idMap, true)
//    var rd: BufferedReader = new BufferedReader(new InputStreamReader(inp))
//    var line: String = null
//    line = rd.readLine()
//    while (line != null && line.length > 0) {
//      val sp = line.trim.split("\\s+")
//      val id = sp(1).toInt
//      if (id != ids.size()) throw new IOException("Error on IDMap line :" + id)
//      ids.add(sp(0))
//      line = rd.readLine()
//    }
//    val idm : Array[String] = new Array[String](ids.size())
//    var i = 0
//    for (elem <- ids) {
//      idm(i) = elem
//      i += 1
//    }
//
//    //ids.clear()
//    logInfo("generate classes")
//    System.out.println("generate classes")
//    inp = CommonFileOperations.openFileForRead(classMap, true)
//    rd = new BufferedReader(new InputStreamReader(inp), 65 * 1024 * 1024)
//    val oup: OutputStream = CommonFileOperations.openFileForWrite(output, true)
//    val pr: PrintWriter = new PrintWriter(new OutputStreamWriter(oup))
//    var l : String = null
//    l = rd.readLine()
//    while (l != null && l.length > 0) {
//      val sp = l.trim.split("\\s+")
//      l = rd.readLine()
//      val id = sp(0).toInt
//      val cls = sp(1).toInt
//      pr.println(idm(id) + "\t" + (cls + 1))
//    }
//    pr.close()
//    rd.close()

    //（word WordID)
    val map : Int2ObjectAVLTreeMap[String] = new Int2ObjectAVLTreeMap[String]()
    map.put(0, "UNK")
    map.put(1, "<s>")
    val ID = sc.sparkContext.textFile(idMap).filter(_.length > 0).map(elem => {
      val sp = elem.trim.split("\\s+")
      (sp(1).toInt, sp(0))
    }).collect()

    for (elem <- ID) {
      map.put(elem._1, elem._2)
    }

//    for (e <- ID)
//      ids.add(e._2)
//
//    val idm : Array[String] = new Array(ids.size())
//    for(i <- 0 until ids.size())
//      idm(i) = ids(i)


    val idsBoradCast = sc.sparkContext.broadcast(map)
    //(wordId||classId||wordCount)
    val clsMap = sc.sparkContext.textFile(classMap, partNum).mapPartitions(part => {
      val idm_ = idsBoradCast.value
      part.map(elem => {
        val split = elem.split("\\s+")
        //val key = idm_.get(split(0).toInt).get
        val key = idm_.get(split(0).toInt)
        key + "\t" + (split(1).toInt + 1)
      })
    })

    logger.info("<1>. " + classMap + "\t and \t" + idMap + "Classes save to " + output)
    System.out.println("<1>. " + classMap + "\t and \t" + idMap + "Classes save to " + output)

    CommonFileOperations.deleteIfExists(output)
    clsMap.repartition(1).saveAsTextFile(output)
  }
}
