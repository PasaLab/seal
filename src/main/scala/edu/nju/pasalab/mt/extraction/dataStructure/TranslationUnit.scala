package edu.nju.pasalab.mt.extraction.dataStructure

import edu.nju.pasalab.mt.extraction.util.alignment.{AlignedSentence, AlignmentTable}
import edu.nju.pasalab.util.{SyntaxType, POSType, DepType, ExtractionParameters}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.{Object2IntOpenHashMap, ObjectArrayList, Object2FloatOpenHashMap}

class TranslationUnit extends Serializable {

  var phraseC: String = ""
  var phraseE: String = ""
  var alignStr: String = ""
  var count: Float = 0
  var relativeFreq: Float = 0
  var lexicalScore: Float = 0
  var totalCount : Float = 0
  val syntaxString : Object2FloatOpenHashMap[String] = new Object2FloatOpenHashMap[String]
  var posTagString : ObjectArrayList[String] = null
  val posTagFrame : Object2FloatOpenHashMap[String] = new Object2FloatOpenHashMap[String]

  var n1plusC : Option[Float] = None
  var n1plusE : Option[Float] = None
  var n1C : Option[Float] = None
  var n2C : Option[Float] = None
  var n3plusC : Option[Float] = None

  def this(allString: String, ep: ExtractionParameters) {
    this()
    val fields: Array[String] = allString.split(" \\|\\|\\| ")
    if (fields.length >= 4) {
      this.phraseC = fields(0)
      this.phraseE = fields(1)
      this.alignStr = fields(2)
      this.count = fields(3).toFloat
      this.relativeFreq = 0
      this.lexicalScore = 0
      var index : Int = 4
      if (ep.syntax == SyntaxType.S2D && fields.length > 4) {
        if (ep.depType == DepType.DepString) {
          val str = fields(index).split(" \\|\\|\\|\\| ")
          //val str  = fields(index)
          index += 1
          for (elem <- str) {
            val spIndex = elem.indexOf("\t")
            if (spIndex > 0) {
              addSyntaxString(elem.substring(spIndex + 1), elem.substring(0, spIndex).toFloat)
            }
          }
        }
        if (fields.length > 5) {
          if (ep.posType == POSType.AllPossible) {
          addPOSTags(fields(index).split(" \\|\\|\\|\\| "))
          index += 1
        } else if (ep.posType == POSType.Frame) {
            val str = fields(index).split(" \\|\\|\\|\\| ")
            index += 1
            for (elem <- str) {
              val spIndex = elem.indexOf("\t")
              if (spIndex > 0) {
                addPOSTagFrame(elem.substring(spIndex + 1), elem.substring(0, spIndex).toFloat)
              }
            }
          }
        }
      }
    }
  }
  def addSyntaxString(str : String, count : Float): Unit = {
    addStringIntoHashMap(str, count)
  }
  def addSyntaxString(str:String): Unit ={
    addSyntaxString(str , 1)
  }
  def addCount(count:Float): Unit = {
    this.count += count
  }
  def mergeSyntaxInfo(unit: TranslationUnit): Unit = {
    if (unit.syntaxString != null) {
      val it = unit.syntaxString.keySet().iterator()
      var key = ""
      while (it.hasNext) {
        key = it.next()
        //println(key, unit.syntaxString.getFloat(key))
        addSyntaxString(key, unit.syntaxString.getFloat(key))
      }
    }
  }
  def mergePOSTagFrame(unit:TranslationUnit): Unit ={
    if(unit.posTagFrame !=  null) {
      val it = posTagFrame.keySet().iterator()
      var key = ""
      while (it.hasNext) {
        key = it.next()
        addStringIntoHashMap(key, unit.posTagFrame.getFloat(key))
      }
    }
  }
  def mergePOSInfo(unit:TranslationUnit): Unit ={
    if (this.posTagString != null && unit.posTagString != null) {

      for (i <- 0 until this.posTagString.size()){
        val posTags:Array[String] = unit.posTagString.get(i).trim().split(" ")
        var init : String = this.posTagString.get(i)
        for(j <- posTags.indices)
          init = insertIntoStringSet(init, posTags(j))
        this.posTagString.set(i, init)
          //= init
      }
    }
  }

  def getAlignedSentence:AlignedSentence = {
    new AlignedSentence(this.phraseC, this.phraseE, this.alignStr)
  }

  def setLexScore(value:Float)={
    this.lexicalScore = value
  }

  def printCombineWeighting(reverseFreq : Float, reverseLexic : Float, withSyntax : Boolean,
                            srcDict : Int2ObjectOpenHashMap[String], tgtDict : Int2ObjectOpenHashMap[String]) : String = {
    val sb: StringBuilder = new StringBuilder(256)
    sb.append(decodePhrase(srcDict, phraseC)).append(SpecialString.mainSeparator)
      .append(decodePhrase(tgtDict, phraseE)).append(SpecialString.mainSeparator)
      ///.append(alignStr).append(SpecialString.mainSeparator)
      .append(reverseFreq).append(" ").append(reverseLexic).append(" ")
      .append(relativeFreq).append(" ").append(lexicalScore).append(" ")
      .append(2.718)
      //.append((0 - 1.0) / count)

    var first: Boolean = true
    if (withSyntax && syntaxString != null && syntaxString.size() > 0) {
      sb.append(SpecialString.mainSeparator)
      val it = syntaxString.keySet().iterator()
      var key = ""
      while (it.hasNext) {
        key = it.next()
        if (first) first = false
        else sb.append(SpecialString.secondarySeparator)
        sb.append(syntaxString.getFloat(key)).append(SpecialString.fileSeparator).append(key)
      }
    }
    if (withSyntax && posTagString != null && posTagString.size() > 0) {
      sb.append(SpecialString.mainSeparator)
      for (i <- 0 until posTagString.size()) {
        if (first) first = false
        else sb.append(SpecialString.secondarySeparator)
        sb.append(posTagString.get(i))
      }
    }
    if (withSyntax && posTagFrame != null) {
      sb.append(SpecialString.mainSeparator)
      val it = posTagFrame.keySet().iterator()
      var key = ""
      while (it.hasNext) {
        key = it.next()
        if (first) first = false
        else sb.append(SpecialString.secondarySeparator)
        sb.append(posTagFrame.getFloat(key)).append(SpecialString.fileSeparator).append(key)
      }

    }
    sb.toString()
  }


  def print(C2E: Boolean, countOnly: Boolean, withSyntax : Boolean,
            srcDict:  Int2ObjectOpenHashMap[String], tgtDict :  Int2ObjectOpenHashMap[String]): String = {

    val sb: StringBuilder = new StringBuilder(256)
    if (C2E) {
      sb.append(decodeRule(srcDict, phraseC)).append(SpecialString.mainSeparator)
        .append(decodeRule(tgtDict, phraseE)).append(SpecialString.mainSeparator)
        .append(alignStr)
    }
    else {
      sb.append(decodeRule(tgtDict, phraseE)).append(SpecialString.mainSeparator)
        .append(decodeRule(srcDict, phraseC)).append(SpecialString.mainSeparator)
        .append(AlignmentTable.getReverseAlignStr(alignStr))
    }

    sb.append(SpecialString.mainSeparator).append(count)

    if (!countOnly) {
      sb.append(SpecialString.mainSeparator).append(relativeFreq)
        .append(" ").append(lexicalScore).append(SpecialString.mainSeparator).append((0-1.0) / count)
    }

    var first: Boolean = true
    if (withSyntax && syntaxString != null) {
      sb.append(SpecialString.mainSeparator)
      val it = syntaxString.keySet().iterator()
      var key = ""
      while (it.hasNext) {
        key = it.next()
        if (first) first = false
        else sb.append(SpecialString.secondarySeparator)
        sb.append(syntaxString.getFloat(key)).append(SpecialString.fileSeparator).append(key)
      }
    }

    if (withSyntax && posTagString != null) {
      sb.append(SpecialString.mainSeparator)
      for (i <- 0 until posTagString.size()) {
        if (first) first = false
        else sb.append(SpecialString.secondarySeparator)
        sb.append(posTagString.get(i))
      }
    }

    if (withSyntax && posTagFrame != null) {
      sb.append(SpecialString.mainSeparator)
      val it = posTagFrame.keySet().iterator()
      var key = ""
      while (it.hasNext) {
        key = it.next()
        if (first) first = false
        else sb.append(SpecialString.secondarySeparator)
        sb.append(posTagFrame.getFloat(key)).append(SpecialString.fileSeparator).append(key)
      }
    }

    sb.toString
  }

  def decodePhrase(dict : Int2ObjectOpenHashMap[String], elem : String) : String = {
    val sp = new StringBuilder(128)
    val split = elem.trim.split("\\s+")
    sp.append(dict.get(split(0).toInt))
    for (i <- 1 until split.length)
      sp.append(" ").append(dict.get(split(i).toInt))
    sp.toString()
  }

  def decodeRule(dict : Int2ObjectOpenHashMap[String], elem : String) : String = {
    val sp = new StringBuilder(128)
    val split = elem.trim.split("\\s+")

    for (e <- elem.trim.split("\\s+")) {
      if (e == "#") sp.append(e).append(" ")
      else if (e.substring(0, 1) == "#" && e.length > 1) sp.append("# ").append(dict.get(e.substring(1).toInt)).append(" ")
      else sp.append(dict.get(e.toInt)).append(" ")

    }
    sp.toString()
  }
  def addStringIntoHashMap(str:String, newCount : Float): Unit = {

    if (str != null) {
      if (this.syntaxString.size() > 0) {
        val it = this.syntaxString.keySet().iterator()
        var key = ""
        while(it.hasNext) {
          key = it.next()
          this.syntaxString.put(key, this.syntaxString.getFloat(key))
        }
      }

      var countA: Float = 0.0f
      if (this.syntaxString.containsKey(str)) countA += this.syntaxString.getFloat(str)
      this.syntaxString.put(str, countA + newCount)
    }
  }
  def insertIntoStringSet(theSet:String, items:String): String = {
    val itemList : Array[String] = items.trim().split(" ")
    val result: StringBuilder = theSet match {
      case _ if theSet != null => new StringBuilder(theSet)
      case _  => new StringBuilder("")
    }
    for (item <- itemList) {
      val tmpKey = " " + item + " "
      if (result.isEmpty)
        result.append(tmpKey)
      else if(!result.toString().contains(tmpKey))
        result.append(item + " ")
    }
    result.toString()
  }
  def addPOSTagFrame(s:String, count:Float): Unit = {
    addStringIntoHashMap(s,count)
  }
  def addPOSTags(pos : Array[String]): Unit = {
    if (this.posTagString == null && pos != null) {
      this.posTagString  = new ObjectArrayList[String](pos.length)
      for (i <- pos.indices) {
        this.posTagString.add("")
      }
    }
    if (pos != null) {
      for (i <- pos.indices)
        this.posTagString.set(i, insertIntoStringSet(this.posTagString.get(i), pos(i)))
    }
  }

  def calculateDependencyProb(): Unit = {
    var total:Float = 0.0f
    if (syntaxString != null) {
      val it = syntaxString.keySet().iterator()
      var key = ""
      while (it.hasNext) {
        key = it.next()
        total += syntaxString.getFloat(key)
      }
      val it1 = syntaxString.keySet().iterator()

      while (it1.hasNext) {
        key = it1.next()
        syntaxString.put(key, syntaxString.getFloat(key) / total)
      }
    }
  }

  def calculatePOSFrameProb(): Unit = {
    var total:Float = 0.0f
    if (posTagFrame != null) {
      val it = posTagFrame.keySet().iterator()
      var key = ""
      while (it.hasNext) {
        key = it.next()
        total += posTagFrame.getFloat(key)
      }
      val it1 = posTagFrame.keySet().iterator()
      while (it1.hasNext) {
        key = it1.next()
        posTagFrame.put(key, posTagFrame.getFloat(key) / total)
      }
    }
  }
}
