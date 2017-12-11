package edu.nju.pasalab.mt.extraction.spark.dataSet

import edu.nju.pasalab.mt.extraction.dataStructure.Splitter
import edu.nju.pasalab.mt.extraction.util.alignment.AlignmentTable
import edu.nju.pasalab.util.tupleEncoders
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by YWJ on 2016.12.31.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object wordTranslationProb extends tupleEncoders {

  /**
    * This is get Word pairs from parallel text
    *
    * @param sc SparkSession Context
    * @param sentenceFileName inputFile name
    * @param partitionNum how much partition we will have
    * @return DataSet[(String , Int)]
    */
  def countWordPairs(sc : SparkSession, sentenceFileName : String, partitionNum : Int) : Dataset[(String, String, Int)] = {
    sc.read.textFile(sentenceFileName).flatMap(line => {
      val sp = line.trim.split(Splitter.mainSplitter)
      try {
        sp(2)
      } catch {
        case e : ArrayIndexOutOfBoundsException => throw new ArrayIndexOutOfBoundsException(line)
      }
      emitWordPair(sp(0), sp(1), sp(2))
    }).repartition(partitionNum)
  }

  import scala.collection._
  def getWordMap(wordPair : Dataset[(String, String, Int)], sc : SparkSession) : Array[mutable.HashMap[String, Object2FloatOpenHashMap[String]]] = {
    import sc.implicits._
    wordPair.alias("wordPairCount").cache()


    val partCountC = wordPair.map(elem => (elem._1, elem._3)).groupByKey(elem => elem._1).mapGroups((k, v) => (k, v.map(x => x._2).sum)).alias("wordCountC")
    val joinedC = wordPair.joinWith(partCountC, $"wordPairCountC._1" === $"wordCountC._1", "inner").mapPartitions(part => {
      part.map(elem => (elem._1._1, elem._1._2, elem._1._3 * 1.0f / elem._2._2))
    })

    val partCountE = wordPair.map(elem => (elem._2, elem._3)).groupByKey(elem => elem._1).mapGroups((k, v) => (k, v.map(x => x._2).sum)).alias("wordCountE")
    val joinedE = wordPair.joinWith(partCountE, $"wordPairCount._2" === $"wordCountE._1", "inner").mapPartitions(part => {
      part.map(elem => (elem._1._1, elem._1._2, elem._1._3 * 1.0f / elem._2._2))
    })

    val prob : Array[mutable.HashMap[String, Object2FloatOpenHashMap[String]]] = new Array[mutable.HashMap[String, Object2FloatOpenHashMap[String]]](2)

    var map : Object2FloatOpenHashMap[String] = null
    joinedC.collect().foreach(entry => {

      if (prob(0).contains(entry._1)) map = prob(0)(entry._1)
      else map = new Object2FloatOpenHashMap[String]()
      map.put(entry._2, entry._3)
      prob(0).put(entry._1, map)
    })

    joinedE.collect().foreach(entry => {
      //var map : Object2FloatOpenHashMap[String] = null
      if (prob(1).contains(entry._2)) map = prob(1)(entry._2)
      else map = new Object2FloatOpenHashMap[String]()
      map.put(entry._1, entry._3)
      prob(1).put(entry._2, map)
    })
    wordPair.unpersist()
    prob
  }


  /**
    * Get Sentence Pair
 *
    * @param srcSentence spurce sentence
    * @param trgSentence target sentence
    * @param alignLine  alignment information
    * @return tuple[rsc, tgt, count]
    */
  def emitWordPair(srcSentence: String, trgSentence: String, alignLine: String): Array[(String, String, Int)] = {
    val wordPairs = new ArrayBuffer[(String, String, Int)]
    val empty = "^\\s*$".r
    if (!empty.findAllMatchIn(srcSentence).hasNext && !empty.findAllMatchIn(trgSentence).hasNext && !empty.findAllMatchIn(alignLine).hasNext) {
      val srcContent = srcSentence.trim.split(" +")
      val trgContent = trgSentence.trim.split(" +")
      val at = new AlignmentTable(srcContent.length, trgContent.length)
      at.fillMatrix_aligntable(alignLine)
      val alignedCountSrc = new Array[Int](srcContent.length)
      val alignedCountTrg = new Array[Int](trgContent.length)
      for (i <- 0 until srcContent.length; j <- 0 until trgContent.length if at.isAligned(i, j)) {
        alignedCountSrc(i) += 1
        alignedCountTrg(j) += 1
        wordPairs += ((srcContent(i), trgContent(i), 1))
      }
      for (i <- 0 until srcContent.length if alignedCountSrc(i) == 0) {
        wordPairs += ((srcContent(i) , "<NULL>", 1))
      }
      for (i <- 0 until trgContent.length if alignedCountTrg(i) == 0) {
        wordPairs += (("<NULL>", trgContent(i), 1))
      }
    }
    wordPairs.toArray
  }
}
