package edu.nju.pasalab.mt.extraction

import edu.nju.pasalab.mt.extraction.dataStructure.{SpecialString, TranslationUnit}
import edu.nju.pasalab.mt.extraction.util.smoothing.{MKNSmoothing, KNSmoothing, GTSmoothing}
import java.util.regex.Pattern
import edu.nju.pasalab.util._

import scala.collection._
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap

class Scoring {

  val aggregationList: Array[TranslationUnit] = null
  var totalCount: Float = 0
  var alignRecorder: mutable.HashMap[String, TranslationUnit] = null
  var probTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]] = null
  var probTableReverse: mutable.HashMap[String, Object2FloatOpenHashMap[String]] = null
  var ep: ExtractionParameters = new ExtractionParameters(ExtractType.Phrase)
  val prule = Pattern.compile("^#\\d*$")

  //for Smoothing
  var GTS : Option[GTSmoothing] = None
  var KNS : Option[KNSmoothing] = None
  var M_KNS : Option[MKNSmoothing] = None

  def this(probTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]], e: ExtractionParameters) {
    //this(e)
    this()
    this.ep = e
    this.probTable = probTable
  }

  def this(probTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]],
           reverseProbTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]], e: ExtractionParameters) {
    //this(e)
    this()
    this.ep = e
//    this.probTable.clear()
//    this.probTable ++= probTable
    this.probTable = probTable
    this.probTableReverse = reverseProbTable
  }

  def this(probTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]],
           reverseProbTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]], gts : GTSmoothing, e: ExtractionParameters) {
    //this(e)
    this()
    this.ep = e
    this.probTable = probTable
    this.probTableReverse = reverseProbTable
    this.GTS = Some(gts)
  }

  def this(probTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]],
           reverseProbTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]], kns : KNSmoothing, e: ExtractionParameters) {
    //this(e)
    this()
    this.ep = e
    this.probTable = probTable
    this.probTableReverse = reverseProbTable
    this.KNS = Some(kns)
  }

  def this(probTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]],
           reverseProbTable: mutable.HashMap[String, Object2FloatOpenHashMap[String]], m_kns : MKNSmoothing, e: ExtractionParameters) {
    //this(e)
    this()
    this.ep = e
    this.probTable = probTable
    this.probTableReverse = reverseProbTable
    this.M_KNS = Some(m_kns)
  }

  def countAggregation_oneline(aUnit: TranslationUnit) {
    if (alignRecorder == null) {
      alignRecorder = new mutable.HashMap[String, TranslationUnit]()
      totalCount = 0
    }

    totalCount += aUnit.count

    if (alignRecorder.contains(aUnit.alignStr))
      alignRecorder(aUnit.alignStr).count += aUnit.count
    else
      alignRecorder += (aUnit.alignStr -> aUnit)
  }

  def calculateReverseTransUnitScoreOnOneUnit(unit: TranslationUnit): TranslationUnit = {
    totalCount = unit.totalCount
    //val maxUnit:TranslationUnit = unit

    if (ep.syntax == SyntaxType.S2D) {
      if (ep.depType == DepType.DepString)
        unit.mergeSyntaxInfo(unit)
      if (ep.posType == POSType.AllPossible)
        unit.mergePOSInfo(unit)
      else if (ep.posType == POSType.Frame)
        unit.mergePOSTagFrame(unit)
    }
    if (ep.allowGoodTuring) {
      val smoothedGroupCount = GTS.get.getSmoothedCount(unit.count)
      unit.relativeFreq = smoothedGroupCount / totalCount
    } else if (ep.allowKN) {
     unit.relativeFreq = KNS.get.getSmoothedProbability(unit.count, totalCount, unit.n1plusC.get, unit.n1plusE.get)
    } else if (ep.allowMKN) {
      unit.relativeFreq = M_KNS.get.getSmoothedProbability(unit.count, totalCount, unit.n1C.get,
        unit.n2C.get, unit.n3plusC.get, unit.n1plusE.get)
    } else {
      unit.relativeFreq = unit.count / unit.totalCount
    }

    var xscore: Float = 1
    val aps = unit.getAlignedSentence
    for (i <- 0 until aps.trgSentence.sentSize) {
      val e = aps.trgSentence.wordSequence(i)
      if (ep.optType != ExtractType.Rule || !prule.matcher(e).matches()) {
        if (!aps.alignment.isTrgAligned(i)) {

          if (probTableReverse.get(SpecialString.nullWordString).isDefined && probTableReverse(SpecialString.nullWordString).containsKey(e))
            xscore *= probTableReverse(SpecialString.nullWordString).getFloat(e)
          else
            xscore *= 1
        } else {
          var scoreW: Float = 0
          val alignedC = aps.alignment.getAlignWords_Trg(i)

          for (aj <- alignedC) {
            val c = aps.srcSentence.wordSequence(aj)

            if (probTableReverse.get(c).isDefined && probTableReverse(c).containsKey(e)) {
              scoreW += probTableReverse(c).getFloat(e)
            } else {
              scoreW += 0
            }
          }
          xscore *= scoreW / alignedC.length
        }
      }
    }
    unit.lexicalScore = xscore
    unit
  }

  def calculateTransUnitScoreOnOneUnit(unit: TranslationUnit): TranslationUnit = {
    totalCount = unit.totalCount

    if (ep.syntax == SyntaxType.S2D) {
      if(ep.depType == DepType.DepString)
        unit.mergeSyntaxInfo(unit)
      if(ep.posType == POSType.AllPossible)
        unit.mergePOSInfo(unit)
      else if(ep.posType == POSType.Frame)
        unit.mergePOSTagFrame(unit)
    }

    if (ep.allowGoodTuring) {
      val smoothedGroupCount = GTS.get.getSmoothedCount(unit.count)
      unit.relativeFreq = smoothedGroupCount / totalCount
    } else if (ep.allowKN) {
      unit.relativeFreq = KNS.get.getSmoothedProbability(unit.count, totalCount, unit.n1plusC.get, unit.n1plusE.get)
    } else if (ep.allowMKN) {
      unit.relativeFreq = M_KNS.get.getSmoothedProbability(unit.count, totalCount, unit.n1C.get,
        unit.n2C.get, unit.n3plusC.get, unit.n1plusE.get)
    } else {

      unit.relativeFreq = unit.count / unit.totalCount
    }

    var xscore: Float = 1
    val aps =  unit.getAlignedSentence


    for (i <- 0 until aps.trgSentence.sentSize) {
      val e = aps.trgSentence.wordSequence(i)
      if (ep.optType != ExtractType.Rule || !prule.matcher(e).matches()) {
        if (!aps.alignment.isTrgAligned(i)) {
          //println(probTable.get(SpecialString.nullWordString).get)
          if (probTable.get(SpecialString.nullWordString).isDefined && probTable(SpecialString.nullWordString).containsKey(e)) {
            //println(probTable(SpecialString.nullWordString).getFloat(e))
            xscore *= probTable(SpecialString.nullWordString).getFloat(e)
          }
          else
            xscore *= 1
        } else {
          var scoreW: Float = 0.0F
          val alignedC = aps.alignment.getAlignWords_Trg(i)

          for (aj <- alignedC) {
            val c = aps.srcSentence.wordSequence(aj)
            //println(probTable.get(c).get)
            if (probTable.get(c).isDefined && probTable(c).containsKey(e)) {
              scoreW += probTable(c).getFloat(e)
            } else {
              scoreW += 0
            }
          }
          xscore *= scoreW / alignedC.length
        }
      }
    }
    if (ep.syntax == SyntaxType.S2D) {
      if (ep.depType == DepType.DepString) {
        unit.calculateDependencyProb()
      }
      if(ep.posType == POSType.Frame)
        unit.calculatePOSFrameProb()
    }
    unit.lexicalScore = xscore
    unit
  }

  def computeLexicalProb(ap: TranslationUnit) : Float ={
    var xscore: Float = 1
    val aps = ap.getAlignedSentence
    for (i <- 0 until aps.trgSentence.sentSize) {
      val e = aps.trgSentence.wordSequence(i)
      if (!aps.alignment.isTrgAligned(i))
        xscore *= probTable(SpecialString.nullWordString).getFloat(e)
      else {
        var scoreW: Float = 0
        val alignedC = aps.alignment.getAlignWords_Trg(i)

        for (aj <- alignedC) {
          val c = aps.srcSentence.wordSequence(aj)
          scoreW += probTable(c).getFloat(e)
        }
        xscore *= scoreW / alignedC.length
      }
    }
    xscore
  }


  /** *
    *
    * @return
    */
  def combineTransUnit(): TranslationUnit = {
    var maxCountAlign: TranslationUnit = null
    var maxCount: Float = Float.MinValue

    for (e <- alignRecorder.keySet) {
      val aUnit = alignRecorder(e)
      if (maxCount < aUnit.count) {
        maxCountAlign = aUnit
        maxCount = aUnit.count
      }
    }
    maxCountAlign.count = totalCount
    alignRecorder.empty
    maxCountAlign
  }
}
