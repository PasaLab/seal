/**
 * This file computes and Stores parameters of Modified Kneser-Ney Smoothing function
 * for N-gram, the Modified Kneser-Ney Smoothing is defined as:
 *       Pmkn(Wi|Wi-n+1 ~ Wi-1) = max{C(Wi-n+1 ~ Wi) - D(C(Wi-n+1 ~ Wi)), 0}/C(Wi-n+1 ~ Wi-1) +
 *                                BOW(Wi-n+1 ~ Wi-1) * Pmkn(Wi|Wi-n+2 ~ Wi-1)
 *       D(c) = {
 *               0      if c = 0
 *               D1     if c = 1
 *               D2     if c = 2
 *               D3+    if c > 2
 *              }
 *       D1 = 1 - 2 * Y * n2/n1
 *       D2 = 1 - 2 * Y * n3/n2
 *       D3+= 1 - 2 * Y * n4/n3
 *       Y = n1 / (n1 + 2 * n2)
 *
 *      BOW(Wi-n+1 ~ Wi-1) = (D1 * N1(Wi-n+1 ~ Wi-1 .) + D2 * N2(Wi-n+1 ~ Wi-1 .) + D3 * N3+(Wi-n+1 ~ Wi-1 .)) / N1+(.Wi-n+1 ~ w-1 .)
 *      N1(Wi-n+1 ~ Wi-1 .) = |{Wi : C(Wi-n+1 ~ Wi} = 1|
 *      N2(Wi-n+1 ~ Wi-1 .) = |{Wi : C(Wi-n+1 ~ Wi} = 2|
 *      N3+(Wi-n+1 ~ Wi-1 .)= |{Wi : C(Wi-n+1 ~ Wi} > 2|
 *
 *      N1+(.Wi-n+1 ~ w-1 .) = C(Wi-n+1 ~ Wi-1)
 * Created by wuyan on 2015/12/28.
 */
package edu.nju.pasalab.mt.LanguageModel.Smoothing

import edu.nju.pasalab.mt.LanguageModel.util.{Splitter, SpecialString}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashSet

class MKNSmoothing extends Serializable{
  var D1 : Option[Double] = None
  var D2 : Option[Double] = None
  var D3 : Option[Double] = None

  def this(data : RDD[(Int, Int)]) {

    this()
    val tmp1 = data.filter(x => x._1 == 1L).collect()
    val tmp2 = data.filter(x => x._1 == 2L).collect()
    val tmp3 = data.filter(x => x._1 == 3L).collect()
    val tmp4 = data.filter(x => x._1 == 4L).collect()
    var n1 : Double = 0.0
    var n2 : Double = 0.0
    var n3 : Double = 0.0
    var n4 : Double = 0.0

    if (tmp1.length > 0) n1 = tmp1(0)._2 * 1.0 else n1 = 1.0
    if (tmp2.length > 0) n2 = tmp2(0)._2 * 1.0 else n2 = 1.0
    if (tmp3.length > 0) n3 = tmp3(0)._2 * 1.0 else n3 = 1.0
    if (tmp4.length > 0) n4 = tmp4(0)._2 * 1.0 else n4 = 1.0

    val y = n1 / (n1 + 2 * n2)
    this.D1 = Some(1 - 2 * y * (n2 / n1))
    this.D2 = Some(1 - 2 * y * (n3 / n2))
    this.D3 = Some(1 - 2 * y * (n4 / n3))

}
  /*
    return D1 D2 D3
   */
  def getD : Array[Double] = { Array(D1.get, D2.get, D3.get) }
  def getD1 : Double = D1.get
  def getD2 : Double = D2.get
  def getD3 : Double = D3.get

  def getSmoothedCountPart1(count : Long , totalCount : Long): Double = {
    val D = if (count == 1)
      D1.get
    else if (count == 2)
      D2.get
    else if(count > 2)
      D3.get
    else 0.0

    val max = Math.max(count * 1.0 - D , 0)
    var result : Option[Double] = None
    if (max == 0)
      result = Some(0.0)
    else
      result = Some(max / (totalCount * 1.0))
    result.get
  }


  def getBackOffWeight(totalCount : Long, N1: Long, N2: Long, N3_ : Long): Double = {
    val molecule = D1.get * N1 + D2.get * N2+ D3.get * N3_
    if (molecule <= 0 || totalCount <= 0)
      0
    else
      molecule / (totalCount * 1.0)
  }

  /**
   * @param gram : gram is the n-gram
   * @return :
   */
  def getLowOrderNum(gram : RDD[(String, Int)]) : RDD[(String, String)] = {
    val num = gram.map(elem => {
      val tmp = elem._1.substring(0, elem._1.lastIndexOf(" "))
      val arr: Array[Long] = new Array[Int](3).map(x => 0L)
      if (elem._2 == 1)
        arr(0) = 1
      else if(elem._2 == 2)
        arr(1) = 1
      else if(elem._2 > 2)
        arr(2) = 1
      (tmp, (elem._2, arr))
    }).reduceByKey((a, b) => {
      val count = a._1 + b._1
      for (i <- 0 until 3)
        a._2(i) += b._2(i)
      (count, a._2)
    }).map(x => {
      (x._1 , x._2._1 + "\t" + x._2._2(0) + "\t" + x._2._2(1) + "\t" + x._2._2(2))
    })
    num
  }

  /**
   * This function is aimed at getting the following value:
   * N1+(.Wi) = |{Wi-1: C(Wi-1 Wi} > 0| is the number of different words that precede wi in the training data
   * N1+(..) is total sum of N1+(.Wj)
   * So for Pkn(Wi):
   *              Pkn(Wi) = N1+(.Wi) / N1+(..)
   * @param bigram : bigram for training data
   * @param count  : is the total number of bigram
   * @return : the Probability of unigram for KN Smoothing
   */
  def getProbabilityOfUnigram(bigram : RDD[(String, Int)], count : Double) :RDD[(String, Double)] = {
    val num = bigram.map(elem => {
      val split = elem._1.split(" ")
      (split(1), elem._1)
    }).reduceByKey((a, b) => {
      a + SpecialString.mainSeparator + b
    }).map(elem => {
      val split = elem._2.split(Splitter.mainSplitter)
      val set : scala.collection.mutable.HashSet[String] = new scala.collection.mutable.HashSet[String]()
      for (e <- split) {
        val arr = e.split(" ")
        set.add(arr(0))
      }
      (elem._1, set.size * 1)
    })
    //    val count = allCount.get * 1.0
    val result = num.map(elem => {
      (elem._1, elem._2 * 1.0 / count)
    })
    result
  }
}
