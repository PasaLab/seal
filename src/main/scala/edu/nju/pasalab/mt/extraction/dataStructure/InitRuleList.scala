package edu.nju.pasalab.mt.extraction.dataStructure

import edu.nju.pasalab.mt.extraction.util.alignment.AlignedSentence

/**
 * Initial rule has no nonTerminal, it's conventional bi-phrases (slot).
 * This class provide a data structure for storing all possible initial rules in a given sentence.
 */
class InitRuleList(aSent: AlignedSentence, val maxInitPhraseLength: Int) {

  val N = aSent.srcSentence.wordSequence.length
  var list:Array[SlotList] = new Array[SlotList](N * (N + 1) / 2)

  /**
   * 上三角矩阵
   */
  def getList(i: Int, j: Int): SlotList = {
    list(i * (2 * N - i + 1) / 2 + j - i)
  }

  def setList(i: Int, j: Int, value: SlotList) {
    list(i * (2 * N - i + 1) / 2 + j - i) = value
  }

  def addRule(slot: Slot) {
    val startC: Int = slot.startC
    val endC: Int = slot.endC
    //val startE: Int = slot.startE
    //val endE: Int = slot.endE
    if (endC - startC >= maxInitPhraseLength || slot.endE - slot.startE >= maxInitPhraseLength) return
    var sList: SlotList = getList(startC, endC)
    if (sList == null) {
      sList = new SlotList
      setList(startC, endC, sList)
    }
    sList.Add(slot)
  }

  def clear()  = list = null
}
