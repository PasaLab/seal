/**
 * This file computes and Stores parameters of Kneser-Ney Smoothing function
 * First you should Know What is Kneser-Ney Smoothing
 * For n-gram :
 *    Pkn(Wi|Wi-n+1 ~ Wi-1) = max{C(Wi-n+1~Wi) - D , 0}/n-gramNum + D/n-gramNu * fresh_num * Pkn(Wi|Wi-n+2 ~ Wi-1)  (1)
 * Note:
 *     D = N1 / (N1 + 2 * N2) , here N1 , N2 are the total number of n-grams with exactly one and two counts
 *     n-gramNum is the number of all n-gram
 *     fresh_num : N1+(Wi-n+1 ~ Wi-1 .) is the number of unique words that follow the words history Wi-n+1 ~ Wi
 *                 N1+(Wi-n+1 ~ Wi-1 .) = |{Wi : C(Wi-n+1 ~ Wi-1 Wi) > 0}|
 *     Function (1) is a recursive function . In order to end this, we take the zeroth-order distribution as the uniform distribution.
 * Created by wuyan on 2015/12/28.
 */
package edu.nju.pasalab.mt.LanguageModel.Smoothing

import edu.nju.pasalab.mt.LanguageModel.util.{Splitter, SpecialString}
import org.apache.spark.rdd.RDD

import scala.collection._

class KNSmoothing extends Serializable{
  var D : Option[Double] = None
  def this(data : RDD[(Int, Int)]) {
    this()
    val tmp1 = data.filter(x => x._1 == 1L).collect()
    val tmp2 = data.filter(x => x._1 == 2L).collect()
    var n1 : Double = 0
    var n2 : Double = 0

    if (tmp1.length > 0) n1 = tmp1(0)._2 * 1.0 else n1 = 1.0
    if (tmp2.length > 0) n2 = tmp2(0)._2 * 1.0 else n2 = 1.0

   /* val n1 = data.filter(x => x._1 == 1L).collect()(0)._2 * 1.0
    val n2 = data.filter(x => x._1 == 2L).collect()(0)._2 * 1.0*/
    val y = n1 / (n1 + 2 * n2)
    this.D = Some(y)
  }

  def getSmoothedCountPart1(groupCount : Int , totalCount : Int) : Double = {
    val max = math.max(groupCount * 1.0 - D.get , 0)
    var result : Option[Double] = None
    if (max == 0.0 || totalCount == 0)
      result = Some(0.0)
    else
      result = Some(max / (totalCount * 1.0))

    result.get
  }

  def getBackOffWeights(freshNum : Int, total : Int) : Double = (D.get * freshNum * 1.0) / (total * 1.0)

  def getD : Double = D.get

  /**
   * This function is aimed at getting the following value:
   * N1+(.Wi) = |{Wi-1: C(Wi-1 Wi} > 0| is the number of different words that precede wi in the training data
   * N1+(..) is total sum of N1+(.Wj)
   * So for Pkn(Wi):
   *              Pkn(Wi) = N1+(.Wi) / N1+(..)
   * @param bigram : bigram for training data
   * @param count  : is the total number of bigram
   * @return : the Probability of 1-gram for KN Smoothing
   */
  def getProbabilityOfUnigram(bigram : RDD[(String, Int)], count : Double) :RDD[(String, Double)] = {
    val num = bigram.map(elem => {
      val split = elem._1.split(" ")
      (split(1), elem._1)
    }).reduceByKey((a, b) => {
      a + SpecialString.mainSeparator + b
    }).map(elem => {
      val split = elem._2.split(Splitter.mainSplitter)
      val set : mutable.HashSet[String] = new mutable.HashSet[String]()
      for (e <- split) {
        val arr = e.split(" ")
        set.add(arr(0))
      }
      (elem._1, set.size)
    })
    val result = num.map(elem => {
      (elem._1, elem._2 * 1.0 / count)
    })
    result
  }


  /**
   * This function is aimed at to get the number of unique words that follow the history Wi-n+1 ~ Wi-1 .
   * N1+(Wi-n+1 ~ Wi-1 .) is the number of unique words that follow the words history Wi-n+1 ~ Wi
   *                 N1+(Wi-n+1 ~ Wi-1 .) = |{Wi : C(Wi-n+1 ~ Wi-1 Wi) > 0}|
   * @param gram  gram is the n-gram
   * @return
   */
  def getUniqueNum(gram : RDD[(String, Int)]) : RDD[(String, String)] = {
    val num = gram.map(elem => {
       val tmp = elem._1.substring(0, elem._1.lastIndexOf(" "))
      (tmp, (elem._1, elem._2))
    }).reduceByKey((a, b) => {
      (a._1 + SpecialString.mainSeparator + b._1, a._2 + b._2)
    }).map(elem => {
      val split = elem._2._1.split(Splitter.mainSplitter)
      val set : mutable.HashSet[String] = new mutable.HashSet[String]()
      for (e <- split) {
        val arr = e.split(" ")
        set.add(arr(arr.length - 1))
      }
      (elem._1, set.size * 1L + "\t" + elem._2._2)
    })
    num
  }

  def getBiGram_BeforeUnique(gram : RDD[(String, Int)]) : RDD[(String, Double)] = {
    val num = gram.mapPartitions(part => {
      part.map(elem => {
        val tmp = elem._1.substring(elem._1.lastIndexOf(" ")+1)
        (tmp, elem._1)
      })
    }).reduceByKey((a, b) => a + SpecialString.mainSeparator + b).map(elem => {
      val split = elem._2.split(Splitter.mainSplitter)
      val set : mutable.HashSet[String] = new mutable.HashSet[String]()
      for (e <- split) {
        set.add(e.substring(0, e.indexOf(" ")))
      }
      (elem._1, set.size * 1.0)
    })
    num
  }
}
