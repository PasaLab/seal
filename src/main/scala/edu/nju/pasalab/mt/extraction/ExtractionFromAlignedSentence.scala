package edu.nju.pasalab.mt.extraction
import edu.nju.pasalab.mt.extraction.dataStructure._
import edu.nju.pasalab.mt.extraction.util.alignment._
import edu.nju.pasalab.mt.extraction.util.dependency.DependencyTree
import edu.nju.pasalab.util._

import scala.collection.mutable.ArrayBuffer

/**
  * The main operations about phrase extraction
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
class ExtractionFromAlignedSentence(val ep: ExtractionParameters) {

  private var aSent: AlignedSentence = null
  private var dt: DependencyTree = null
  private var alignedCountC: Array[Int] = null
  private var alignedCountE: Array[Int] = null
  private var alignedIdxE: Array[Array[Int]] = null

  private var maxELength = 0
  private var maxCLength = 0

  private var countC: Int = 0
  private var countE: Int = 0

  private var _phraseList: ArrayBuffer[ExtractedPhrase] = null
  private var _initRuleList: InitRuleList = null
  private var _ruleSet: RuleSet = null

  var _curSlotList: SlotList = new SlotList()
  var _curSlotCount = 0

  var pos: Array[String] = null

  /** *
    * set parameter for this class construct
    * Performing extraction with dependency and postag information.
    * This function initializes the parameters and internal data structures and calls ExtractTranslationUnit() and ExtractNullPhrase() to extract.
 *
    * @param as alignment sentence
    * @param dt dependency tree information
    * @param POS target pos tags
    */
  def extractSentence(as: AlignedSentence, dt:DependencyTree, POS:Array[String]) {

    if (ep.optType == ExtractType.Phrase) {
      maxCLength = ep.maxc
      maxELength = ep.maxe
    } else if (ep.optType == ExtractType.Rule) {
      maxCLength = ep.maxSpan
      maxELength = ep.maxSpan
    } else if (ep.optType == ExtractType.PhraseWithReordering) {
      //TODO:PhraseWithReordering

    }

    countC = as.srcSentence.sentSize
    countE = as.trgSentence.sentSize
    this.aSent = as
    this.dt = dt
    this.pos = POS

    alignedCountC = new Array[Int](countC)
    alignedCountE = new Array[Int](countE)
    alignedIdxE = new Array[Array[Int]](countE)

    // set alignment structure for extraction
    for (i <- 0 until countC)
      alignedCountC(i) = aSent.alignment.getAlignWords_Src(i).length
    for (i <- 0 until countE) {
      val list: Array[Int] = aSent.alignment.getAlignWords_Trg(i)
      alignedIdxE(i) = list
      alignedCountE(i) = list.length
    }

    // main extraction operation
    if (ep.optType == ExtractType.Phrase || ep.optType == ExtractType.PhraseWithReordering) {
      _phraseList = new ArrayBuffer[ExtractedPhrase]()
      extractTranslationUnit()
      if (ep.withNullOption)
        _phraseList ++= extractNullPhrase()
    } else if (ep.optType == ExtractType.Rule) {
      _initRuleList = new InitRuleList(aSent, ep.maxInitPhraseLength)
      _ruleSet = new RuleSet(aSent)
      extractTranslationUnit()
    }
  }

  def getPhraseList(): Array[ExtractedPhrase] = _phraseList.toArray

  def getRuleSet():RuleSet = _ruleSet

  /** *
    * The main operation to extract phrase
    */
  def extractTranslationUnit() {
    var noTree:Boolean = false
    if (dt == null) {
      noTree = true
      val parent:Array[Int] = new Array[Int](aSent.trgSentence.wordSequence.length)
      parent.map(elem => -1)
      dt = new DependencyTree(aSent.trgSentence.wordSequence, parent)
    }
    if (pos == null) {
      pos = new Array[String](aSent.trgSentence.wordSequence.length)
      pos.map(elem => "X")
    }

    for {len <- 1 to countE if len <= maxELength
         lenE = countE - len
         startE <- 0 to lenE} {

      val endE = startE + len - 1
      //println("len: "+ len +"; startE: "+startE + "; endE: "+ endE)

      var isWellFormedSpan = 0
      var spanType: String = null
      var head = -1
      var headPOS: String = null
      var syntaxString: String = null
      if (ep.syntax == SyntaxType.S2D) {
        var isWellFormed = false
        var isFixed = false
        var isLeftFloating = false
        var isRightFloating = false
        var other = false

        head = dt.IsFixed(startE, endE + 1, false)
        if (dt.IsFixed(startE, endE + 1, head)) { //fixed
          isFixed = true
          isWellFormed = true
        } else {//floating
          head = dt.IsFloating(startE, endE + 1, false)
          if (ep.constrainedType != ConstrainedType.WellFormed) {
            if (dt.IsLeftFloating(startE, endE + 1, head)) {
              isLeftFloating = true
              isWellFormed = true
            } else if(dt.IsRigthFloating(startE, endE + 1, head)) {
              isRightFloating = true
              isWellFormed = true
            }
          }
        }
        if (!isWellFormed) {
          head = dt.IsFixed(startE, endE + 1, true)
          if (dt.IsFixed(startE, endE + 1, head)) {
            isFixed = true
          } else {
            head = dt.IsFloating(startE, endE + 1, true)
            if (ep.constrainedType != ConstrainedType.Relaxed) {
              if (dt.IsLeftFloating(startE, endE + 1, head)) {
                isLeftFloating = true
              } else if (dt.IsRigthFloating(startE, endE + 1, head)) {
                isRightFloating = true
              } else
                other = true
            }
          }
        }
        if (noTree)
          spanType = "other"
        else if (isFixed)
          spanType = "fixed"
        else if(isLeftFloating)
          spanType = "leftFloating"
        else if(isRightFloating)
          spanType = "rightFloating"
        else
          spanType = "other"

        //constructing the syntaxString, only needed for Type DepWithHeadPOS, and DepString
        if (ep.depType == DepType.DepString) {
          val syntaxStringBuilder:StringBuilder = new StringBuilder(64)
          syntaxStringBuilder.append(spanType)
          syntaxStringBuilder.append("\t")
          if (isWellFormed)
            isWellFormedSpan = 0
          else
            isWellFormedSpan = 1
          syntaxStringBuilder.append(isWellFormedSpan)
          syntaxStringBuilder.append("\t")
          val headList:Array[Int] = dt.getSubtree(startE, endE + 1)
          syntaxStringBuilder.append(headList(0))
          for (i <- 1 until headList.length){
            syntaxStringBuilder.append(" ")
            syntaxStringBuilder.append(headList(i))
          }
          syntaxString = syntaxStringBuilder.toString()
        }
        //get the pos tag list
        if (ep.posType != POSType.None && pos.length >= 0){
          //noly one frame
          if (spanType.equals("fixed"))
            headPOS = pos(head)
          else if ((spanType.equals("leftFloating") || spanType.equals("rightFloating")) && ep.keepFloatingPOS)
            headPOS = pos(head)
          else
            headPOS = "X"
        }
      }
      //step 1 : for span (startE, endE)
      var minC: Int = Int.MaxValue
      var maxC: Int = Int.MinValue

      val usedC: Array[Int] = new Array[Int](countC)
      for (i <- 0 until countC)
        usedC(i) = alignedCountC(i)

      if (ep.allowBoundaryUnaligned || (alignedCountE(startE) != 0 && alignedCountE(endE) != 0)) {

        //step 2: retrieval its aligned source phrase
        for (ei <- startE to endE) {
          if (alignedCountE(ei) != 0) {
            for (fi <- alignedIdxE(ei)) {
              if (fi < minC) minC = fi
              if (fi > maxC) maxC = fi
              usedC(fi) -= 1
            }
          }
        }

        //step 3: check consistency
        //check if aligned phrase is null and satisfies the max_length constraint.
        if (maxC >= 0 && maxC - minC < maxCLength) {
          var isConsistent = true
          for (ci <- minC to maxC)
            if (usedC(ci) > 0) isConsistent = false

          if (isConsistent) {
            var startC = minC
            while (startC >= 0 && startC <= minC && (startC == minC || alignedCountC(startC) == 0 && ep.allowBoundaryUnaligned)) {
              var endC = maxC
              while (endC < countC && endC >= maxC && (endC == maxC || alignedCountC(endC) == 0 && ep.allowBoundaryUnaligned)) {

                if (ep.optType == ExtractType.Phrase && endC - maxCLength < startC && startC + maxCLength > maxC) {

                  val aPhrase: ExtractedPhrase = new ExtractedPhrase(aSent, startE, endE, startC, endC)
                  aPhrase.addTargetDepInfo(syntaxString, headPOS)
                  _phraseList += aPhrase

                } else if (ep.optType == ExtractType.Rule && endC - maxCLength < startC && startC + maxCLength > maxC) {

                  val parentSlot: Slot = new Slot(startE, endE, startC, endC, 0, isWellFormedSpan, spanType, head, headPOS)
//                  println("startE: " + startE + " endE :" + endE + " startC: " + startC + " endC: " + endC + " isWellFormedSpan: " 
//                      + isWellFormedSpan + " sanType: " + spanType + " head :" + head + " headPOS : " + headPOS)
                  _initRuleList.addRule(parentSlot)
//                  println("Add InitRule startE: " + startE + ";endE: " + endE + ";startC: " + startC + "; endC: " + endC)
                  _curSlotList = new SlotList
                  _curSlotCount = 0
//                  println("begin find hole................................................")
                  AddHieroRule(parentSlot, startC.toShort, endC - startC + 1, endE - startE + 1)
//                  println("hole found over..................................................")
                  _curSlotList = null

                }
//               println("phrase loop: startC "+startC+" endC "+endC)
                endC += 1

              }
              startC -= 1
            }
          }
        }
      }
    }
  }

  def AddHieroRule(parentSlot: Slot, curSlotCStart: Int, cWordsCount: Int, eWordsCount: Int) {
    //val startC = parentSlot.startC
    //val startE = parentSlot.startE
    //val endC = parentSlot.endC
    //val endE = parentSlot.endE
    val lengthC: Int = parentSlot.getLengthC
//    println("call addhieroRule***********************************")
    if (_curSlotCount != ep.maxSlots) {

      for (len <- ep.minWordsInSlot until lengthC) {
        var startHC = curSlotCStart


        while (startHC + len <= parentSlot.endC + 1) {

          val endHC = startHC + len - 1
//          println("startHC: "+startHC + " endHC: "+endHC + " startC: " + startC + " endC: " + endC)

          if (!(startHC == parentSlot.startC && endHC == parentSlot.endC)) {

            val newCWordsCnt = cWordsCount - len

            if (!(newCWordsCnt < ep.minCWords || _curSlotCount == ep.maxSlots - 1 && newCWordsCnt + (_curSlotCount + 1) > ep.maxCSymbols)) {
              val slotsWithCurCSpan: SlotList = _initRuleList.getList(startHC, endHC)

              if (slotsWithCurCSpan != null) {
//                println("newCWordsCnt: " + newCWordsCnt + " listen : " + slotsWithCurCSpan.sList.size())
                val listLen = slotsWithCurCSpan.sList.size()

                for (index <-0 until  listLen) {
                  val slot = slotsWithCurCSpan.sList.get(index)
                  val newEWordsCnt = eWordsCount - slot.getLengthE
                  val startHE = slot.startE
                  val endHE = slot.endE

//                  println("startHE: "+startHE + " endHE: "+endHE)

                  if (!(newEWordsCnt < ep.minEWords
                    || slot.getLengthE < ep.minWordsInSlot
                    || _curSlotCount == ep.maxSlots - 1 && newEWordsCnt + (_curSlotCount + 1) > ep.maxESymbols
                    || startHE < parentSlot.startE || endHE > parentSlot.endE)) {

                    if (!_curSlotList.IsOverlapWith(slot)) {

                      if (!(!ep.allowConsecutive && _curSlotList.IsNeighborWith(slot))) {

                        var isAligned = true
                        if (!ep.allowAllUnaligned) {
                          var iter = 0
                          isAligned = false
                          var i: Int = parentSlot.startC
                          while (i <= parentSlot.endC ) {
                            if (i == startHC) i = endHC
                            else if (iter < _curSlotCount && i == _curSlotList.sList.get(iter).startC) {
                              i = _curSlotList.sList.get(iter).endC
                              iter += 1
                            }
                            else if (alignedCountC(i) > 0) {
                              isAligned = true
                              i = parentSlot.endC
                            }
                            i += 1
                          }

                        }
                        if (isAligned) {
                          _curSlotCount += 1
                          val newSlot: Slot = new Slot(startHE, endHE, startHC, endHC, _curSlotCount, slot.isWellFormSpan, slot.slotType, slot.head, slot.headPOS)
                          _curSlotList.Add(newSlot)
                          if (newCWordsCnt + _curSlotCount <= ep.maxCSymbols && newEWordsCnt + _curSlotCount <= ep.maxESymbols) {
                            _ruleSet.addRule(new ExtractedRule(aSent, pos, ep.depType, ep.posType, parentSlot, _curSlotList, dt))
                          }
                         //short newSlotCStart = (ep.allowConsecutive) ? (short) (endHC + 1) : (short) (endHC + 2);
                          val newSlotCStart: Short = if (ep.allowConsecutive) (endHC + 1).toShort else (endHC + 2).toShort
                          AddHieroRule(parentSlot, newSlotCStart,  newCWordsCnt,newEWordsCnt)
                          _curSlotList.Pop
                          _curSlotCount -= 1
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          startHC += 1
        }
      }
    }
  }

  /** *
    * Extract words that translated into NULL (WordTranslationTable.nullWordString).
 *
    * @return
    */
  def extractNullPhrase(): Array[ExtractedPhrase] = {
    val result: ArrayBuffer[ExtractedPhrase] = ArrayBuffer[ExtractedPhrase]()
    //TODO: extractNullPhrase
    for (j <- 0 until countC) {
      //word that have no aligned words && the words before and after it have aligned words
      var flag: Boolean = true
      if (alignedCountC(j) == 0) {
        if (j > 0 && alignedCountC(j - 1) == 0)
          flag = false
        if (j < countC - 1 && alignedCountC(j + 1) == 0)
          flag = false
      } else flag = false

      if (flag) {
        val ap: ExtractedPhrase = new ExtractedPhrase(aSent.srcSentence.wordSequence(j), SpecialString.nullWordString, SpecialString.nullAlignString, SpecialString.nullAlignString, 1)
        if (ep.syntax == SyntaxType.S2D){
          var syntaxString:String = null
          var posTags:String = null
          if (ep.depType == DepType.DepString)
            syntaxString = "fixed\t-1"
          if(ep.posType != POSType.None)
            posTags = "X"
          ap.addTargetDepInfo(syntaxString, posTags)
        }
        result += ap
      }
    }

    for (j <- 0 until countE) {
      var flag: Boolean = true
      if (alignedCountE(j) == 0) {
        if (j > 0 && alignedCountE(j - 1) == 0)
          flag = false
        if (j < countE - 1 && alignedCountE(j + 1) == 0)
          flag = false
      } else flag = false

      if (flag) {
        val ap: ExtractedPhrase = new ExtractedPhrase(SpecialString.nullWordString, aSent.trgSentence.wordSequence(j), SpecialString.nullAlignString, SpecialString.nullAlignString, 1)
        if (ep.syntax == SyntaxType.S2D) {
          var syntaxString:String = null
          var posTags:String = null
          if (ep.depType == DepType.DepString)
            syntaxString = "fixed\t-1"
          if(ep.posType != POSType.None)
            posTags = pos(j)
          ap.addTargetDepInfo(syntaxString, posTags)
        }
        result += ap
      }
    }
    result.toArray
  }
}
