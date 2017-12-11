package edu.nju.pasalab.mt.extraction.util.smoothing

/**
 * Created by  on 2015/12/1.
 * This file computes and Stores parameters of Kneser-Ney Smoothing function
 * First you should Know What is Kneser-Ney Smoothing
 *
 */
class KNSmoothing {
  var D1 : Option[Float] = None
  var allCountC : Option[Float] = None
  var allCountE : Option[Float] = None

  def this(n1 : Float, n2 : Float, allCountC : Float, allCountE: Float) {
    this()
    val y : Float = n1 / (n1 + 2 * n2)
    D1 = Some(y)
    this.allCountC = Some(allCountC)
    this.allCountE = Some(allCountE)
  }

  /**
   * Description of method getSmoothedProbability: Using mkn-smoothing method to compute relative frequency p(s|t).
   * groupCount: c(s,t)
   * totalCount: c(t)
   * n1plusE: the number of phrase t for which c(s,t)>0
   * n1plusC: the number of phrase s for which c(s,t)>0
   */
  def getSmoothedProbability(groupCount : Float, totalCount : Float, n1plusC : Float, n1plusE : Float) : Float = {
    val d : Float = D1.get
    val alpt :Float = d * n1plusC / totalCount

    val pbst : Float = n1plusE / allCountE.get

    (groupCount - d) / totalCount + alpt * pbst
  }
}
