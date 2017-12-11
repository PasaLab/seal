package edu.nju.pasalab.mt.LanguageModel.Smoothing

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.nju.pasalab.mt.LanguageModel.util._
import edu.nju.pasalab.util.math.BIGDECIMAL
import org.apache.spark.rdd.RDD

/**
 * This file computes and Stores parameters of Good-Turing Smoothing function
 * First you should Know What is GoodTuring Smoothing and Why we need this ?
 * we need to explain some term we will use in the next !!!
 * r:  representation frequency of a word or unit in a corpus
 * Nr: representation the frequency of frequency
 *
 * The Log-log line Function is : Log(Nr) = para1 * Log(r) + para2     (function1)
 * This function aims at handling the problem that the number of r+1 will equal to zero when r is very large
 * Here We use linear least squares to compute parameters and error term is : r * (Log(Nr) - Log(Nr')) * (Log(Nr) - Log(Nr'))
 * Function2: Standard Good Turing Smoothing Function
 * Pr = (1/N) * r*
 * r* = (r+1)E(Nr+1)/E(Nr)
 * Created by wuyan on 2015/12/22.
 */
class GTSmoothing extends Serializable {
  var para1:Option[Double] = None
  var para2:Option[Double] = None
  var all_r:Option[Long] = None
  var N1 : Option[Long] = None
  var index : Option[IndexedRDD[Int, Int]] = None

  val stopSmoothingThreshold = 1000
  def this(r_Nr : RDD[(Int, Int)]) {
    this()
    val arr = getParameters(r_Nr, stopSmoothingThreshold)
    //this.index = Some(IndexedRDD(r_Nr))
    this.para1 = Some(arr(0))
    this.para2 = Some(arr(1))
    this.all_r = Some(getAllCount(r_Nr))
    this.N1 = Some(r_Nr.filter(_._1 == 1).map(_._2).collect()(0))
  }

  /**
    * @param r_Nr frequency and frequency of frequency
    * @return total number of n-gram
    */
  def getAllCount(r_Nr : RDD[(Int, Int)]):Long = r_Nr.map(x => x._1 * x._2).reduce(_+_)

  /**
    * use linear least square to get coefficient for function Log(Nr) = para1 * Log(r) + para2
    *
    * @param r_Nr array
    * @param stopSmoothingThreshold  threshold value for smoothing
    * @return the coefficient
    */
  def getPara(r_Nr : RDD[(Long, Int)], stopSmoothingThreshold : Int): Array[Double] = {
    val result: Array[Double] = new Array[Double](2)
    //LLS: linear least square
    val LLS = r_Nr.mapPartitions(part =>{
      part.map(elem => {
        val arr : Array[Double] = new Array[Double](5)
        if (elem._1 < stopSmoothingThreshold) {
          arr(0) = elem._1
          arr(1) = elem._1 * math.log(elem._1) * math.log(elem._2)
          arr(2) = elem._1 * math.log(elem._1) * math.log(elem._1)
          arr(3) = elem._1 * math.log(elem._1)
          arr(4) = elem._1 * math.log(elem._2)
        }
        arr
      })
    }).reduce((a, b) => {
      val arr : Array[Double] = new Array[Double](5)
      arr(0) = a(0) + b(0)
      arr(1) = a(1) + b(1)
      arr(2) = a(2) + b(2)
      arr(3) = a(3) + b(3)
      arr(4) = a(4) + b(4)
      arr
    })
    result(0) = (LLS(0) * LLS(1) - LLS(3) * LLS(4)) / (LLS(0) * LLS(2) - LLS(3) * LLS(3))
    result(1) = (LLS(4) - result(0) * LLS(3)) / LLS(0)
    result
  }

  /**
    * 0: sum(logNr*logr)
    * 1: sum(logNr)
    * 2: sum(logr)
    * 3: sum(logr)^2^
 *
    * @param rNr array
    * @param stopSmoothingThreshold threshold value for smoothing
    * @return
    */
  def getParameters(rNr: RDD[(Int, Int)], stopSmoothingThreshold: Int): Array[Double] = {
    val result: Array[Double] = new Array[Double](2)
    val m = rNr.count()
    val lls = rNr.map(elem => {
      val arr : Array[Double] = new Array[Double](5)
      if (elem._1 < stopSmoothingThreshold) {
        arr(0) = math.log(elem._1 * 1.0) * math.log(elem._2 * 1.0)
        arr(1) = math.log(elem._2 * 1.0)
        arr(2) = math.log(elem._1 * 1.0)
        arr(3) = math.log(elem._1 * 1.0) * math.log(elem._1 * 1.0)
      }
      arr
    }).reduce((a, b) => {
      val arr : Array[Double] = new Array[Double](4)
      arr(0) = a(0) + b(0)
      arr(1) = a(1) + b(1)
      arr(2) = a(2) + b(2)
      arr(3) = a(3) + b(3)
      arr
    })
    result(0) = Divide(m * lls(0) - lls(0) * lls(1), m * lls(3) - lls(2) * lls(2))
    result(1) = Divide(lls(1) - result(0) * lls(2), m * 1.0)
    result
  }
  /**
    * this function use The Log-log line Function is : Log(Nr) = para1 * Log(r) + para2 to get the adjusted Count
 *
    * @param r is the frequency of a word
    * @return adjustedCount for r
    */
  def getSmoothedCount(r : Long) : Double = {
    var result : Double = 0
    if (r > stopSmoothingThreshold)
      result = r * 1.0
    else if (r == 0)
      result = N1.get * 1.0
    else
       //result = (r + 1) * math.exp(para1.get * math.log(r + 1) + para2.get) / math.exp(para1.get * math.log(r) + para2.get)
       result = r * math.pow( 1 + (1.0 / r*1.0), para1.get)
    result
  }

  /**
    * this function is aimed at use GT directly to compute the adjustedCount for r
    *
    * @param r is the frequency of a word
    * @return the adjustedCount for r
    */
  def getSmoothedCountD(r : Int) :Double ={
    var result :Double = 0.0
    if (r > stopSmoothingThreshold)
      result = r * 1.0
    else if (r == 0)
      result = N1.get * 1.0
    else {
      result = (r+1) * (index.get.get(r+1).get*1.0 / index.get.get(r).get*1.0)
    }
    result
  }

  def getUniProb(count : Double) : Double = count / all_r.get*1.0


  def getTotalSmoothedCount(totalCount: Double, totalGTCount : Double) : Double = {
    var result : Double = 0
    if (totalCount > stopSmoothingThreshold)
      result = totalCount
    else
      result = totalGTCount + (totalCount / all_r.get) * math.exp(para2.get)
    result
  }

  /**
   * this function is aimed at merging the (n-1)-gram and n-gram
    *
    * @param gram 2-gram
   * @return
   */
  def mergeGramCount(gram : RDD[(String, Long)]) : RDD[(String, Long)] = {
    val result = gram.mapPartitions(part => {
      part.map(elem => {
        val tmp = elem._1.substring(0, elem._1.lastIndexOf(" "))
        Array((tmp, 1L), (elem._1, elem._2))
      })
    }).flatMap(x => x)
    result
  }


  def Divide(a : Double, b : Double) : Double = {
    BIGDECIMAL.divide(a, b)
  }
}
