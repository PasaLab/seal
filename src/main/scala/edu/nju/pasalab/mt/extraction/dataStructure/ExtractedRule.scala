package edu.nju.pasalab.mt.extraction.dataStructure


import edu.nju.pasalab.mt.extraction.util.alignment.{AlignedSentence, AlignmentTable}
import edu.nju.pasalab.mt.extraction.util.dependency.DependencyTree
import edu.nju.pasalab.util.{POSType, DepType}
import DepType.DepType
import POSType.POSType

/**
 * This is the data structure for hierarchical rule.
 * It includes the lexical string of the source/target side of the phrase,
 * information about the non-terminals (X),
 * the alignment string, and other information like syntax and pos-tags.
 */
class ExtractedRule {
  var slot: Slot = null
  var sList: SlotList = null
  var lhs: String = null
  var rhs: String = null
  var alignStr: String = null
  var invAlignStr: String = null
  var useDepString = false
  var useHeadPOS = false

  var deptype: DepType = null
  var postype: POSType = null
  var dt: DependencyTree = null
  var aSent: AlignedSentence = null
  var posTag: Array[String] = null
  var syntaxString: String = null
  var nonTerminalTags: String = null

  var CPosToInd: Array[Int] = null
  var EPosToInd: Array[Int] = null

  var CSlotToInd: Array[Int] = null
  var ESlotToInd: Array[Int] = null

  def this(aSent: AlignedSentence, hole: Slot, hList: SlotList) {
    this()
    init(aSent, hole, hList)
  }

  def this(aSent: AlignedSentence, deptype: DepType, hole: Slot, hList: SlotList) {
    this()
    init(aSent, hole, hList)
  }
  def this(aSent:AlignedSentence,dt:DependencyTree, deptype:DepType,hole:Slot, hList:SlotList){
    this()
    if (dt != null) {
      this.dt = dt
      this.deptype = deptype //type should be DepType.DepString, because no POSTags are provide
      this.useDepString =
        if(this.deptype == DepType.DepString) true else false
      init(aSent, hole, hList)
    }
  }

  //
  def this(aSent: AlignedSentence, postags: Array[String], deptype: DepType, postype: POSType, hole: Slot, hList: SlotList, dt: DependencyTree) {
    this()
    if (dt != null) {
      this.dt = dt
      this.deptype = deptype
      this.postype = postype
      if (this.deptype == DepType.DepString)
        this.useDepString = true
        
      if (postags != null ){
        //use pos tags in this setting requires dependency tree
        this.posTag = postags
        if (this.postype != POSType.None)
          this.useHeadPOS = true
      }
    }
//    println("start init:")
//    for(elem <- postags) {
//      System.out.print(elem + " ")
//    }
//    println()
//    //class Slot(val startE:Int,val endE:Int,val startC:Int,val endC:Int,val index:Int, val isWellFormSpan:Int, val slotType:String, val head:Int,val headPOS:String
//    println("startE: " + hole.startE + " endE :" + hole.endE + " startC: " + hole.startC + " endC: " + hole.endC + " isWellFormedSpan: " 
//                      + hole.isWellFormSpan + " sanType: " + hole.slotType + " head :" + hole.head + " headPOS : " + hole.headPOS)
    init(aSent, hole, hList)
  }

  private[dataStructure] def init(aSent: AlignedSentence, slot: Slot, sList: SlotList) {
    this.aSent = aSent
    this.slot = slot
    this.sList = sList
    CPosToInd = new Array[Int](slot.getLengthC)
    EPosToInd = new Array[Int](slot.getLengthE)
    CSlotToInd = new Array[Int](slot.getLengthC)
    ESlotToInd = new Array[Int](slot.getLengthE)
    buildLHS
    buildRHS
    buildAlignment
    buildInvAlignment
    
    if (this.useDepString) buildSyntaxString
    if (this.useHeadPOS) buildPOSString
  }

  def buildAlignment {
    // Lemma : There is no link linking from out-hole word to in-hole word.
    // on basis of Och's phrase extraction assumption.
    val sb: StringBuilder = new StringBuilder(128)
    val startE = slot.startE
    //val endE = slot.endE

    for (i <- startE to slot.endE if EPosToInd(i - startE) != 0) {
      val alignedC: Array[Int] = aSent.alignment.getAlignWords_Trg(i)
      for (j <- alignedC) {
        sb.append(CPosToInd(j - slot.startC) - 1 + "-" + (EPosToInd(i - startE) - 1) + " ")
      }
    }
    alignStr = sb.toString.trim
  }

  def buildInvAlignment {
    val sb: StringBuilder = new StringBuilder(64)
    val startC = slot.startC
    val endC = slot.endC
    for (j <- startC to endC if CPosToInd(j - startC) != 0) {
      val alignedE: Array[Int] = aSent.alignment.getAlignWords_Src(j)
      for (i <- alignedE) {
        sb.append((EPosToInd(i - slot.startE) - 1) + "-" + (CPosToInd(j - startC) - 1) + " ")
      }
    }
    invAlignStr = sb.toString.trim
  }

  def buildLHS {//replace the slot with "#"
    val startC = slot.startC
    val endC = slot.endC
    val holeCount: Int = sList.getCount
    var iter: Int = 0
    var index: Int = 1
    var curSlotPos = sList.sList.get(iter).startC
    lhs = null
    val sb = new StringBuilder(64)

    var i = startC
    while (i <= endC) {
      if (iter < holeCount && i == curSlotPos) {
        CSlotToInd(i - startC) = index
        i = sList.sList.get(iter).endC
        iter = iter + 1
        if (iter < holeCount) curSlotPos = sList.sList.get(iter).startC
        sb.append("# ")
      }
      else {
        CPosToInd(i - startC) = index
        sb.append(aSent.srcSentence.wordSequence(i) + " ")
      }
      index += 1
      i = i + 1
    }
    lhs = sb.toString.trim
  }

  def buildRHS {
    val sortedHListByE: SlotList = sList.GetSortedListByE
    val startE = slot.startE
    val endE = slot.endE
    val holeCount: Int = sList.getCount
    var iter: Int = 0
    var index: Int = 1
    var curSlotPos = sortedHListByE.sList.get(iter).startE
    val sb: StringBuilder = new StringBuilder(64)
    iter = 0
    curSlotPos = sortedHListByE.sList.get(iter).startE
    var i: Int = startE
    while (i <= endE) {
      if (iter < holeCount && i == curSlotPos) {
        ESlotToInd(i - startE) = index
        sb.append("#")
        sb.append(sortedHListByE.sList.get(iter).index)
        sb.append(" ")
        i = sortedHListByE.sList.get(iter).endE
        iter += 1
        if (iter < holeCount) curSlotPos = sortedHListByE.sList.get(iter).startE
      }
      else {
        EPosToInd(i - startE) = index
        sb.append(aSent.trgSentence.wordSequence(i))
        sb.append(" ")
      }
      index += 1
      i = i + 1
    }
    rhs = sb.toString.trim
  }

  def buildSyntaxString: String = {
    val sortedHListByE: SlotList = sList.GetSortedListByE
    val startE = slot.startE
    val endE = slot.endE
    val holeCount: Int = sList.getCount
    var iter: Int = 0
    var curSlotPos = sortedHListByE.sList.get(iter).startE
    val sb: StringBuilder = new StringBuilder(64)
    sb.append(this.slot.slotType)
    sb.append("\t")
    sb.append(this.slot.isWellFormSpan)
    sb.append("\t")
    val epos2indx: scala.collection.mutable.HashMap[Int, Int] = new scala.collection.mutable.HashMap[Int, Int]
    var indx: Int = 0
    var i: Int = startE
    while (i <= endE) {
      if (iter < holeCount && i == curSlotPos) {
        var j: Int = sortedHListByE.sList.get(iter).startE
        while (j <= sortedHListByE.sList.get(iter).endE) {
          epos2indx += (j -> indx)
          j += 1
        }
        indx += 1
        i = sortedHListByE.sList.get(iter).endE
        iter += 1
        if (iter < holeCount) 
          curSlotPos = sortedHListByE.sList.get(iter).startE
      }
      else {
        epos2indx += (i ->indx)
        indx += 1
      }
      i += 1
    }
    iter = 0
    curSlotPos = sortedHListByE.sList.get(iter).startE
    i = startE
    while (i <= endE) {
      var headOfCurrent: Int = 0
      if (iter < holeCount && i == curSlotPos) {
        val currentSlot: Slot = sortedHListByE.sList.get(iter)
        i = currentSlot.endE
        headOfCurrent = currentSlot.head
        if (headOfCurrent >= currentSlot.startE && headOfCurrent <= currentSlot.endE && dt.parent.length >= 0){
          headOfCurrent = dt.parent(headOfCurrent)
        }
        iter += 1
        if (iter < holeCount) 
          curSlotPos = sortedHListByE.sList.get(iter).startE
      }
      else if(i < dt.parent.length) headOfCurrent = dt.parent(i)
      if (headOfCurrent > endE || headOfCurrent < startE) 
        sb.append("-1 ")
      else {
        sb.append(epos2indx(headOfCurrent))
        sb.append(" ")
      }
      i += 1
    }
    syntaxString = sb.toString.trim
    syntaxString
  }

  def buildPOSString {
    if (postype eq POSType.AllPossible) {
      nonTerminalTags = this.slot.headPOS
      import scala.collection.JavaConversions._
      for (s <- sList.sList) nonTerminalTags += SpecialString.secondarySeparator + s.headPOS
    }
    else if (postype eq POSType.Frame) {
      nonTerminalTags = this.slot.headPOS
      import scala.collection.JavaConversions._
      for (s <- sList.sList) nonTerminalTags += SpecialString.listItemSeparator + s.headPOS
    }
  }

  def clear {
    CPosToInd = null
    EPosToInd = null
    CSlotToInd = null
    ESlotToInd = null
    slot = null
  }


  override def equals(o:Any): Boolean = {
    val rule: ExtractedRule = o.asInstanceOf[ExtractedRule]
    rhs.equals(rule.rhs) && lhs.equals(rule.lhs) && !(slot.startC > rule.slot.endC || slot.endC < rule.slot.startC)
  }

  override def hashCode: Int = {
    toString.hashCode
  }

  override def toString: String = {
    lhs + rhs + alignStr
  }

  def print(C2E: Boolean): String = {
    print(C2E, 1)
  }

  def print(C2E: Boolean, count: Float): String = {
    val sb: StringBuilder = new StringBuilder(256)
    if (C2E) {
      sb.append(lhs)
      sb.append(SpecialString.mainSeparator)
      sb.append(rhs)
      sb.append(SpecialString.mainSeparator)
      sb.append(alignStr)
      sb.append(SpecialString.mainSeparator)
      sb.append(count)
    }
    else {
      sb.append(rhs)
      sb.append(SpecialString.mainSeparator)
      sb.append(lhs)
      sb.append(SpecialString.mainSeparator)
      sb.append(invAlignStr)
      sb.append(SpecialString.mainSeparator)
      sb.append(count)
    }
    if (deptype eq DepType.DepString) {
      sb.append(SpecialString.mainSeparator)
      sb.append("1\t")
      sb.append(syntaxString)
    }
    if (postype eq POSType.AllPossible) {
      sb.append(SpecialString.mainSeparator)
      sb.append(nonTerminalTags)
    }
    else if (postype eq POSType.Frame) {
      sb.append(SpecialString.mainSeparator)
      sb.append("1\t")
      sb.append(nonTerminalTags)
    }
    sb.toString
  }
}
object ExtractedRule{
  /**
   * Make sure that lhs has hiero tag #, and rhs has tag with number, i.e. #1 #2
   * For example: change the rule "#1 reply ||| # 的 回信 同样"
   * to "#1 reply ||| # 的 回信 同样"
   * The order and corespodence between tags should be kept.
   */
  def reverseHieroTag(phrasePair:String): String = {
    val phraseSplit = phrasePair.split(SpecialString.mainSeparator)
    val src = phraseSplit(0)
    val trg = phraseSplit(1)
    var newsrc: StringBuilder = null
    var newtrg: StringBuilder = null
    val index1: Int = src.indexOf("#1")
    val index2: Int = src.indexOf("#2")
    if (index1 == -1) return phrasePair
    else if (index2 == -1) return src.replace("#1", "#")+SpecialString.mainSeparator+trg.replaceFirst("#", "#1")
    else {
      newsrc = new StringBuilder(src)
      newtrg = new StringBuilder(trg)
      val index1_trg: Int = newtrg.indexOf("#")
      val index2_trg: Int = newtrg.indexOf("#", index1_trg + 1)
      if (index1 < index2) {
        newsrc.deleteCharAt(index2 + 1)
        newsrc.deleteCharAt(index1 + 1)
        newtrg.insert(index2_trg + 1, '2')
        newtrg.insert(index1_trg + 1, '1')
      }
      else {
        newsrc.deleteCharAt(index1 + 1)
        newsrc.deleteCharAt(index2 + 1)
        newtrg.insert(index2_trg + 1, '1')
        newtrg.insert(index1_trg + 1, '2')
      }
    }
    newsrc.append(SpecialString.mainSeparator).append(newtrg)
    newsrc.toString()
  }
}
