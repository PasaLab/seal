package edu.nju.pasalab.mt.extraction.util.alignment


class Sentence(val sentStr: String) {

  val wordSequence: Array[String] = sentStr.split(" ")
  val sentSize: Int = wordSequence.length

  def getSubstring(startWordIndex: Int, endWordIndex: Int): String = {
    val result: StringBuilder = new StringBuilder(64)
    if (checkWordIndex(startWordIndex) && checkWordIndex(endWordIndex)) {
      for (i <- startWordIndex until endWordIndex) {
        result.append(wordSequence(i))
        result.append(" ")
      }
      result.append(wordSequence(endWordIndex))
      result.toString
    }
    else null
  }

  def checkWordIndex(wordIndex: Int): Boolean = {
    if (wordIndex < 0 || wordIndex >= wordSequence.length) false
    else true
  }
}
