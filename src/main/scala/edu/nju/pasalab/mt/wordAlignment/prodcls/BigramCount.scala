package edu.nju.pasalab.mt.wordAlignment.prodcls

import java.io.IOException
import edu.nju.pasalab.mt.util.CommonFileOperations
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by YWJ on 2016.6.6.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object BigramCount{

  val logger:Logger  = LoggerFactory.getLogger(BigramCount.getClass)

  def buildBigram(digitCorpus : RDD[String] , temp : String, output : String,
                  clsMap : RDD[String], cls : Int, sc : SparkSession): RDD[(Long, Long)] = {
    /**
      * broadcast class map
      * id classID count
      * Class Map --> id classID count
      */

    val map : Int2IntOpenHashMap = new Int2IntOpenHashMap
    val col = clsMap.filter(_.length > 0).map(line => {
      val sp = line.trim.split("\\s+")
      (sp(0).toInt, sp(1).toInt)
    }).collect()
    for (elem <- col) {
      map.put(elem._1, elem._2)
    }
    val mapBroadcast = sc.sparkContext.broadcast(map)

    //(key1 || key2 || classID || count)
    val bigram = get_NGram(digitCorpus).mapPartitions(part => {
      val tmp = mapBroadcast.value
      part.map(elem => {
        val key = elem._1.trim.split("\\s+")(1).toInt
        val cls = tmp.get(key)
//        cls match {
//          case _ if (cls == 0) => throw new IOException("Bad class mapping " + key + " entry : " + elem._1)
//        }
        val sb = new StringBuilder(elem._1).append(" ")
        sb.append(cls).append(" ").append(elem._2)
        sb.toString()
      })
    })

    logger.info("<1>. bigram save to " + temp)
    System.out.println("<1>. bigram save to " + temp)
    CommonFileOperations.deleteIfExists(temp)
    bigram.saveAsTextFile(temp)

    //encodeKey || count
    val out = bigram.mapPartitions(part => {
      part.map(elem => {
        val splits = elem.trim.split("\\s+")
//        splits match {
//          case _ if (splits.length != 4) => throw new IOException("BAD INPUT! MUST HAVE 4 Fields")
//          case _ if (splits(2).toLong > cls) => throw new IOException("BAD class id > nclass! " + splits(2))
//        }
        (splits(0).toLong * (cls + 1) + splits(2).toLong, splits(3).toLong)
      })
    }).reduceByKey(_+_)

    logger.info("<2>. Class Bigram  save to " + output)
    System.out.println("<2>. Class Bigram save to " + output)
    CommonFileOperations.deleteIfExists(output)
    out.map(x => x._1 + "\t" + x._2).saveAsTextFile(output)
    out
  }

  //bigram count
  def get_NGram(data : RDD[String]):RDD[(String, Int)] = {
    val result = data.mapPartitions(part => {
      part.map(elem => {
        val split = elem.trim.split("\\s+")
        val arr : ArrayBuffer[(String, Int)] = new ArrayBuffer[(String, Int)]()
        arr.append(("1 " + split(0), 1))
        for (i <- 1 until split.length)
          arr.append((split(i - 1) + " " + split(i), 1))
        arr
      })
    }).flatMap(x => x).reduceByKey(_+_)
    result
  }

  def encodeKey(v : Long, cw : Long, cls : Int) : Long = {
    v * (cls + 1) + cw
  }
}
