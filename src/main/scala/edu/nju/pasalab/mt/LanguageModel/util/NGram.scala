package edu.nju.pasalab.mt.LanguageModel.util

import java.io.{OutputStreamWriter, BufferedWriter, PrintWriter}

import edu.nju.pasalab.mt.util.{SntToCooc, CommonFileOperations}
import edu.nju.pasalab.util.ExtractionParameters
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by YWJ on 2015/12/28.
  *  Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
object NGram {
  /**
   * @param data is the Set of input
   * @param n   is the value of n-gram
   * @return    n-gram
   */
  def get_NGram(data : RDD[String], n : Int ): RDD[(String, Int)] = {
    val result = data.mapPartitions(part => {
      part.map(elem => {
        val split = elem.split(" ")
        val arr : ArrayBuffer[(String, Int)] = new ArrayBuffer[(String, Int)]()
        var i = 0
        var j = i + n - 1
        while(((i + n - 1) < split.length) && (j < split.length)) {
          val tmp = new StringBuilder(128)
          tmp.append(split(i))
          for (index <- i+1 until j+1)
            tmp.append(" ").append(split(index))
          arr.append((tmp.toString(), 1))
          tmp.clear()
          i += 1
          j += 1
        }
        arr
      })
    }).flatMap(x => x).reduceByKey(_+_)

    result
  }

  def getGrams(trainData : RDD[String], N : Int): Array[RDD[(String, Int)]] = {
    val grams = new Array[RDD[(String, Int)]](N)
    for (i <- 0 until N) {
      grams(i) = get_NGram(trainData, i+1)
    }
    grams
  }

  def getCount(grams :Array[RDD[(String, Int)]], N: Int):Array[Long] = {
    val arr = new Array[Long](N)
    for (i <- 0 until N) {
      arr(i) = grams(i).count()
    }
    arr
  }

  /**
    * Special Token : <s> </s> unk : we given id : 0 1 2
    *
    * @param data training data
    * @param lmRootDir language model working directory
    * @return
    */
  def encodeString(sc : SparkSession, data : RDD[String], lmRootDir : String) : RDD[String] = {
    println("Build Dict & encoding line . ")
    val sorted = data.flatMap(line => line.trim.split("\\s+"))
      .map(x => (x, 1))
      .reduceByKey(_+_)
      .collect().sortWith(_._2 > _._2)

    CommonFileOperations.deleteIfExists(SntToCooc.getLMDictPath(lmRootDir))

    val dict : Object2IntOpenHashMap[String] = new Object2IntOpenHashMap()
    val dictFile : PrintWriter = new PrintWriter(new BufferedWriter(
      new OutputStreamWriter(CommonFileOperations.openFileForWrite(SntToCooc.getLMDictPath(lmRootDir), true)), 129 * 1024 * 1024))

    dict.put("<s>", 0)
    dict.put("</s>", 1)
    dict.put("unk", 2)

    dictFile.println("<s> " + 0)
    dictFile.println("</s> " + 1)
    dictFile.println("unk " + 2)

    var id = 3
    for(elem <- sorted) {
      dict.put(elem._1, id)
      dictFile.println(elem._1 + " " + id)
      id += 1
    }
    dictFile.close()

    val idBroadcast = sc.sparkContext.broadcast(dict)
    val res = data.mapPartitions(part => {
      val id = idBroadcast.value
      part.map(elem => {
        val sb = new StringBuilder(128)
        val split = elem.trim.split("\\s+")
        sb.append(0)
        for (elem <- split) {
          sb.append(" ").append(id.getInt(elem))
        }
        sb.append(" ").append(1)

        sb.toString()
      })
    })
    res
  }

}