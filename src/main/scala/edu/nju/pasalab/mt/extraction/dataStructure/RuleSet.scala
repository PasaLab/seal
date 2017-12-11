package edu.nju.pasalab.mt.extraction.dataStructure
import edu.nju.pasalab.mt.extraction.util.alignment.AlignedSentence
class RuleSet {
  var allowFractionalCount = false
  var ruleCount : java.util.HashSet[ExtractedRule] = null
  var spanCount:Array[Array[Array[Array[Int]]]] = null

  def this(aSent: AlignedSentence) {
    this()
    ruleCount = new java.util.HashSet[ExtractedRule]
    if (allowFractionalCount) spanCount = new Array[Array[Array[Array[Int]]]](aSent.srcSentence.wordSequence.length)
  }

  def clear {
    if (ruleCount != null) {
      val it = ruleCount.iterator()
      while(it.hasNext){
        val r = it.next()
        r.clear
      }
      ruleCount.clear
      ruleCount = null
    }
  }

  def addRule(rule: ExtractedRule) {
    ruleCount.add(rule)
  }

  def print(C2E: Boolean): java.util.ArrayList[String] = {
    allowFractionalCount match {
      case true => {
        getSpanCount
        printFractionalCount(C2E)
      }
      case false => printConventionalCount(C2E)
    }
  }

  def getSpanCount {
    val it = ruleCount.iterator()
    while(it.hasNext){
      val r = it.next()
      spanCount(r.slot.startC)(r.slot.endC)(r.slot.startE)(r.slot.endE) += 1
      spanCount(r.slot.startC)(r.slot.endC)(r.slot.startE)(r.slot.endE) - 1
    }
  }

  def printFractionalCount(C2E: Boolean): java.util.ArrayList[String] = {
    val results: java.util.ArrayList[String] = new java.util.ArrayList[String]
    import scala.collection.JavaConversions._
    for (r <- ruleCount) {
      //val count: Float =
      val sC2E: String = r.print(C2E, 1.0F / spanCount(r.slot.startC)(r.slot.endC)(r.slot.startE)(r.slot.endE))
      results.add(sC2E)
    }
    results
  }

  def printConventionalCount(C2E: Boolean): java.util.ArrayList[String] = {
    val results: java.util.ArrayList[String] = new java.util.ArrayList[String]
    import scala.collection.JavaConversions._
    for (rule <- ruleCount) {
      val sC2E: String = rule.print(C2E)
      results.add(sC2E)
    }
    results
  }


}


