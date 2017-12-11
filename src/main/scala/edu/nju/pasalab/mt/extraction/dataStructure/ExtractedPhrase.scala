package edu.nju.pasalab.mt.extraction.dataStructure

import edu.nju.pasalab.mt.extraction.util.alignment.AlignedSentence
import edu.nju.pasalab.util.{POSType, DepType}
import edu.nju.pasalab.util.DepType
import DepType.DepType
import POSType.POSType

/**
 * This class is the data structure for phrase.
 */
class ExtractedPhrase(startC: Int,
                      endC: Int,
                      startE: Int,
                      endE: Int,
                      val phraseC: String,
                      val phraseE: String,
                      val count: Float) {

  var score: Float = -1
  var alignStr : String = ""
  var invAlignStr : String = ""
  var reorderInfo : String = ""
  var syntaxString : String = ""
  var posTagString : String = ""

  def this(parent: AlignedSentence, startE: Int, endE: Int, startC: Int, endC: Int) {
    this(startC, endC, startE, endE, parent.srcSentence.getSubstring(startC, endC),
      parent.trgSentence.getSubstring(startE, endE), 1)
    this.alignStr = getAlignment(parent, this.startE, this.endE, this.startC, this.endC)
    this.invAlignStr = getInvAlignment(parent, this.startE, this.endE, this.startC, this.endC)
    this.reorderInfo = getReorderInfo(parent, this.startE, this.endE, this.startC, this.endC)

  }

  def this(c: String, e: String, a: String, ia: String, count: Float) {
    this(0, 0, 0, 0, c, e, count)
    this.alignStr = a
    this.invAlignStr = ia
  }

  def getReorderInfo(parent: AlignedSentence, startE: Int, endE: Int, startC: Int, endC: Int):String ={
    val bf = new StringBuilder(128)

    val connectedLeftTop = parent.alignment.isAligned(startC-1,startE-1)
    val connectedRightTop = parent.alignment.isAligned(endC+1,startE-1)
    if (connectedLeftTop && !connectedRightTop)
      bf.append("m")
    else if (!connectedLeftTop && connectedRightTop)
      bf.append("s")
    else
      bf.append("o")

    val connectedLeftBottom = parent.alignment.isAligned(startC-1,endE+1)
    val connectedRightBottom = parent.alignment.isAligned(endC+1,endE+1)
    if(connectedLeftBottom && !connectedRightBottom)
      bf.append(" t")
    else if(!connectedLeftBottom && connectedRightBottom)
      bf.append(" n")
    else
      bf.append(" p")

    bf.toString
  }

  def getAlignment(parent: AlignedSentence, startE: Int, endE: Int, startC: Int, endC: Int): String = {
    val sb: StringBuilder = new StringBuilder(64)
    var isfirst: Boolean = true
    for (i <- startE to endE) {
      val alignedC: Array[Int] = parent.alignment.getAlignWords_Trg(i)
      for (ai <- alignedC) {
        if (isfirst) isfirst = false
        else sb.append(" ")
        sb.append(ai - startC)
        sb.append("-")
        sb.append(i - startE)
      }
    }
    sb.toString
  }

  def addTargetDepInfo(syn:String, tags:String): Unit ={
    this.syntaxString = syn
    this.posTagString = tags
  }
  def printCount(C2E: Boolean, depType: DepType, posType:POSType): String = {
    val sb_count: StringBuilder = new StringBuilder(128)
    if (C2E) {
      sb_count.append(phraseC)
      sb_count.append(SpecialString.mainSeparator)
      sb_count.append(phraseE)
      sb_count.append(SpecialString.mainSeparator)
      sb_count.append(alignStr)
      sb_count.append(SpecialString.mainSeparator)
      sb_count.append(count)
    } else {
      sb_count.append(phraseE)
      sb_count.append(SpecialString.mainSeparator)
      sb_count.append(phraseC)
      sb_count.append(SpecialString.mainSeparator)
      sb_count.append(invAlignStr)
      sb_count.append(SpecialString.mainSeparator)
      sb_count.append(count)
    }
    if (depType == DepType.DepString) {
      sb_count.append(SpecialString.mainSeparator)
      sb_count.append("1\t")
      sb_count.append(syntaxString)
    }
    if (posType == POSType.AllPossible) {
      sb_count.append(SpecialString.mainSeparator)
      sb_count.append(posTagString)
    } else if (posType == POSType.Frame) {
      sb_count.append(SpecialString.mainSeparator)
      sb_count.append("1\t")
      sb_count.append(posTagString)
    }
    sb_count.toString
  }

  def getInvAlignment(parent: AlignedSentence, startE: Int, endE: Int, startC: Int, endC: Int): String = {
    val sb: StringBuilder = new StringBuilder(64)
    var isfirst: Boolean = true
    for (j <- startC to endC) {
      val alignedE: Array[Int] = parent.alignment.getAlignWords_Src(j)
      for (aj <- alignedE) {
        if (isfirst) isfirst = false
        else sb.append(" ")
        sb.append(aj - startE)
        sb.append("-")
        sb.append(j - startC)
      }
    }
    sb.toString
  }
}
