package edu.nju.pasalab.mt.LanguageModel.util

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
  * Created by YWJ on 2016/3/20.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object Indexing {
  /**
    * This function is aimed at to create an unify indexing for word sequence of n-gram
    * @param data is the universal set, it certainly contains training set and test set
    * @param n is the value the N
    * @return the index for each n-gram
    */
  def createIndex(data : RDD[String], n : Int ): Array[RDD[(String, Long)]] = {
    val result : Array[RDD[(String, Long)]] = new Array[RDD[(String, Long)]](n)
    for (index <- 1 until (n+1)) {
      result(index-1) = data.mapPartitions(part => {
        part.map(elem => {
          val split = elem.split(" ")
          val arr : ArrayBuffer[String] = new ArrayBuffer[String]()
          var i = 0
          var j = i + index - 1
          while(((i + index - 1) < split.length) && (j < split.length)) {
            val sb = new StringBuilder(256)
            sb.append(split(i))
            for (index <- i+1 until j+1)
              sb.append(" ").append(split(index))
            arr.append(sb.toString())
            i += 1
            j += 1
          }
          arr
        })
      }).flatMap(x => x).distinct().zipWithUniqueId()
    }
    result
  }
}
