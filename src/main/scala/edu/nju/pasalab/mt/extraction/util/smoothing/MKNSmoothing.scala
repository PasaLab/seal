package edu.nju.pasalab.mt.extraction.util.smoothing

/**
 * Created by  on 2015/12/1.
 * This file computes and Stores parameters of Kneser-Ney Smoothing function
 * First you should Know What is Kneser-Ney Smoothing
 *
 */
class MKNSmoothing {
  var D1 : Option[Float] = None
  var D2 : Option[Float] = None
  var D3 : Option[Float] = None
  var allCount : Option[Float] = None

  def this(n1 : Float, n2 : Float, n3 : Float, n4: Float, allCount : Float) {
    this()
    val y = n1 / (n1 + 2 * n2)

    D1 = Some(1 - 2 * y * (n2 / n1))
    D2 = Some(2 - 3 * y * (n3 / n2))
    D3 = Some(3 - 4 * y * (n4 / n3))
    this.allCount = Some(allCount)
  }

  /*
   * Description of method getSmoothedProbability: Using mkn-smoothing method to compute relative frequency p(s|t).
   * @param groupCount: c(s,t)
   * @param totalCount: c(t)
   * @param n1plusE: the number of phrase t for which c(s,t) > 0
   * @param n1C: the number of phrase s for which c(s,t) = 1
   * @param n2C: the number of phrase s for which c(s,t) = 2
   * @param n3plusC: the number of phrase s for which c(s,t) >= 3
   */
  def getSmoothedProbability(groupCount : Float, totalCount : Float, n1C : Float, n2C : Float, n3plusC : Float, n1plusE : Float) : Float = {
    var d : Float = 0
    if (groupCount == 1)
      d = D1.get
    else if (groupCount == 2)
      d = D2.get
    else
      d = D3.get

    val alpt : Float = (D1.get * n1C + D2.get * n2C + D3.get * n3plusC) / totalCount
    val pbst : Float = n1plusE / allCount.get

    (groupCount - d) / totalCount + alpt * pbst
  }
}
