package edu.nju.pasalab.mt.extraction.util.smoothing

import it.unimi.dsi.fastutil.floats.FloatArrayList

/**
 * Created by  on 2015/12/1.
 * This file computes and Stores parameters of Good-Turing Smoothing function
 * First you should Know What is GoodTuring Smoothing and Why we need this ?
 * we need to explain some term we will use in the next !!!
 * r:  representation frequency of a word or unit in a corpus
 * Nr: representation the frequency of frequency
 *
 * The Log-log line Function is : Log(Nr) = para1 * Log(r) + para2 (function1)
 * This function aims at handling the problem that the number of r+1 will equal to zero when r is very large
 * Here We use linear least squares to compute parameters and error term is : r * (Log(Nr) - Log(Nr')) * (Log(Nr) - Log(Nr'))
 * Function2: Standard Good Turing Smoothing Function
 * Pr = (1/N) * r*
 * r* =(r+1)E(Nr+1)/E(Nr)
 * N is the total number of the unit
 */
class GTSmoothing {

  var para1:Option[Float] = None
  var para2:Option[Float] = None
  var all_r:Option[Float] = None

//  private var para1 : Option[Float] = None
//  private var para2 : Option[Float] = None
//  private var all_r : Option[Float] = None
  // 1000 ia a threshold which limits the smoothing range
  val stopSmoothingThreshold : Int = 1000

  def this(r : FloatArrayList, Nr : FloatArrayList){
    this()
    this.para1 = Some(getPara1(r, Nr))
    this.para2 = Some(getPara2(r, Nr, this.para1.get))
    this.all_r = Some(getAllCount(r, Nr))
  }

  def getAllCount(r : FloatArrayList, Nr : FloatArrayList): Float = {
    var all_r : Float = 0
    for (i <- 0 until r.size())
      all_r += r.getFloat(i) * Nr.getFloat(i)
    /*for (i <- r.indices)
      all_r += r(i) * Nr(i)*/
    all_r
  }

  def getPara1(r : FloatArrayList, Nr : FloatArrayList): Float = {
    //var para1: Float = 0
    var all_r : Float = 0
    var all_r_Nr : Float = 0
    var all_r_Logr2 : Float = 0
    var all_r_Logr : Float = 0
    var all_r_LogNr : Float = 0

    for (i <- 0 until r.size()) {
      if (r.getFloat(i) < stopSmoothingThreshold) {
        all_r_Nr += (Math.log(r.getFloat(i)) * Math.log(Nr.getFloat(i)) * r.getFloat(i)).toFloat
        all_r += r.getFloat(i)
        all_r_Logr2 += (r.getFloat(i) * Math.log(r.getFloat(i)) * Math.log(r.getFloat(i))).toFloat
        all_r_Logr += (r.getFloat(i) * Math.log(r.getFloat(i))).toFloat
        all_r_LogNr += (Math.log(Nr.getFloat(i)) * r.getFloat(i)).toFloat
      }
    }
    /*for (i <- r.indices) {
      if (r(i) < stopSmoothingThreshold) {
        all_r_Nr += (Math.log(r(i)) * Math.log(Nr(i)) * r(i)).toFloat
        all_r += r(i)
        all_r_Logr2 += (r(i) * Math.log(r(i)) * Math.log(r(i))).toFloat
        all_r_Logr += (r(i) * Math.log(r(i))).toFloat
        all_r_LogNr += (Math.log(Nr(i)) * r(i)).toFloat
      }
    }*/
    (all_r * all_r_Nr - all_r_Logr * all_r_LogNr) / (all_r * all_r_Logr2 - all_r_Logr * all_r_Logr)
    //para1
  }

  def getPara2(r : FloatArrayList, Nr : FloatArrayList, para1 : Float): Float = {
    var para2: Float = 0
    var all_r: Float = 0
    var all_r_logr: Float = 0
    var all_r_logNr: Float = 0

    for (i <- 0 until r.size()) {
      if (r.getFloat(i) < stopSmoothingThreshold) {
        all_r += r.getFloat(i)
        all_r_logr = (r.getFloat(i) * Math.log(r.getFloat(i))).toFloat
        all_r_logNr += (r.getFloat(i) * Math.log(Nr.getFloat(i))).toFloat
      }
    }
/*    for (i <- r.indices) {
      if (r(i) < stopSmoothingThreshold) {
        all_r += r(i)
        all_r_logr += (r(i) * Math.log(r(i))).toFloat
        all_r_logNr += (r(i) * Math.log(Nr(i))).toFloat
      }
    }*/
    para2 = (all_r_logNr - para1 * all_r_logr) / all_r
    para2
  }

  def getSmoothedCount(r : Float) : Float = {
    var result: Float = 0
    if (r > stopSmoothingThreshold)
      result = r
    else {
      result = ((r + 1) * Math.exp(para1.get * Math.log(r + 1) + para2.get) / Math.exp(para1.get * Math.log(r) + para2.get)).toFloat
    }
    result
  }

  def getSmoothedTotalCount(totalCount : Float, totalGTCount : Float) : Float = {
    var result: Option[Float] = None
    if (totalCount > stopSmoothingThreshold)
      result = Some(totalCount)
    else
      result = Some(totalGTCount + (totalCount / all_r.get) * Math.exp(para2.get).toFloat)
    result.get
  }

  override  def toString : String  = para1.get + "\t" + para2.get + "\t" + all_r
}
