/**
  * Created by wuyan on 2015/12/28.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
package edu.nju.pasalab.mt.LanguageModel.util

import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, HashSet}
object toolFun {

  /**
   * @param str : represent the sentence
   * @param n   : represent the value of the n-gram. such 3-gram , n = 3
   * @return    : return the Sentence N-gram Array as the following format:
   *                                                                n-gram ||| (n-1)-gram
   */
  def getSentence(str : String, n : Int) : Array[String] = {
    val split = str.split(" ")
    val arr : ArrayBuffer[String] = new ArrayBuffer[String]()
    /**
     * First We Should get the word Sequence
     * For a sentence S composed of the words "w1w2w3..wl", we can express P(S) as :
     *  P(S) = P(w1|<BOS>) * P(w2|<BOS>w1) * ... * P(<EOS>|<BOS>w1...wl)
     *  we make the approximation that the probability of a word only depend on the identity of
     *  the immediately preceding n-1 words, give us :
     *
     *  for (i <- 1 until l + 1)
     *    P(S) *= P(Wi|Wi-n+1 ~ Wi-1)
     *
     * Note: <BOS> is the beginning of the Sentence and the <EOS> is the end of the Sentence
     */
    arr.append(split(0))
    var i  = 1
    while (i < split.length ) {
      val builder = new StringBuilder(128)
      var begin = i - n + 1
      val end = i - 1
      while (begin < 0) begin += 1
      builder.append(split(begin))
      for (index <- begin+1 until end + 1) {
        builder.append(" ").append(split(index))
      }
      builder.append(" ").append(split(i))
      arr.append(builder.toString())
      i += 1
    }
    arr.toArray
  }

  /**
   * This function is aimed at to get the number of unique words that follow the history Wi-n+1 ~ Wi-1 .
   * N1+(Wi-n+1 ~ Wi-1 .) is the number of unique words that follow the words history Wi-n+1 ~ Wi
   *                 N1+(Wi-n+1 ~ Wi-1 .) = |{Wi : C(Wi-n+1 ~ Wi-1 Wi) > 0}|
   * @param gram : gram is the n-gram
   * @return :
   *         String (Long, Long)
   *         word  (fresh_num, total_number)
   */
  def getUniqueNum(gram : RDD[(String, Long)]) : RDD[(String, String)] = {
    val num = gram.map(elem => {
      val tmp = elem._1.substring(0, elem._1.lastIndexOf(" "))
      (tmp, (elem._1, elem._2))
    }).reduceByKey((a, b) => {
      (a._1 + SpecialString.mainSeparator + b._1, a._2 + b._2)
    }).map(elem => {
      val split = elem._2._1.split(Splitter.mainSplitter)
      val set : scala.collection.mutable.HashSet[String] = new scala.collection.mutable.HashSet[String]()
      for (e <- split) {
        val arr = e.split(" ")
        set.add(arr(arr.length - 1))
      }
      (elem._1, set.size * 1L + "\t" + elem._2._2)
    })
    num
  }
}
