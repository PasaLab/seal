package edu.nju.pasalab.mt.extraction.spark.exec

import java.util.regex.Pattern
import edu.nju.pasalab.mt.extraction.util.smoothing.GTSmoothing
import edu.nju.pasalab.mt.util.{CommonFileOperations, SntToCooc}
import edu.nju.pasalab.util._
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.objects.{ObjectArrayList, Object2FloatOpenHashMap}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.slf4j.{LoggerFactory, Logger}

import edu.nju.pasalab.mt.extraction.util.dependency.DependencyTree
import edu.nju.pasalab.mt.extraction.ExtractionFromAlignedSentence
import edu.nju.pasalab.mt.extraction.dataStructure.{Splitter, SpecialString}
import edu.nju.pasalab.mt.extraction.util.alignment.AlignedSentence
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object RuleExtraction {
  val logger:Logger  = LoggerFactory.getLogger(RuleExtraction.getClass)
  val empty:Pattern = Pattern.compile("^\\s*$")
  val preserve : Boolean = true

  def extract_rule(sc: SparkSession, ep: ExtractionParameters): RDD[String] = {

    val sentence_library = sc.sparkContext.textFile(SntToCooc.getHDFSTMPath(ep.rootDir), ep.partitionNum)
      .flatMap(sent_pair => {
      val content = sent_pair.split(Splitter.mainSplitter)
      val as = new AlignedSentence(content(0).trim, content(1).trim, content(2).trim)
      var dt:DependencyTree = null
      var pos:Array[String] = null
      if (content.length > 3) {
         if (ep.syntax == SyntaxType.S2D) {
          if (!((content.length < 4 || empty.matcher(content(3)).matches()) && ep.constrainedType != ConstrainedType.ANY)) {
            val dep:Array[Int] = content(3).trim.split(" ").map(elem => elem.toInt)
            dt = new DependencyTree(as.trgSentence.wordSequence, dep)
            if (ep.posType != POSType.None) {
              if(!((content.length < 5 || empty.matcher(content(4)).matches()) && ep.constrainedType != ConstrainedType.ANY))
                pos = content(4).trim.split(" ")
            }
          }
        }
      }
      val e = new ExtractionFromAlignedSentence(ep)
      e.extractSentence(as, dt, pos)

      val list = e.getRuleSet().ruleCount
      val result: Array[(String, String)] = new Array[(String, String)](list.size)
      val it = list.iterator()
      var i = 0
      while(it.hasNext){
        val r = it.next()
        val resultKey = new StringBuilder(64)
        val resultValue = new StringBuilder(128)
        
        resultKey.append(r.lhs).append(SpecialString.mainSeparator).append(r.rhs).append(SpecialString.mainSeparator)
        .append(r.alignStr)
        
        resultValue.append(1)

        if (content.length > 3) {
          if (ep.depType == DepType.DepString){
            if (r.syntaxString != null) {
              resultValue.append(SpecialString.mainSeparator).append("1\t").append(r.syntaxString)
            }
          }
          if (ep.posType == POSType.AllPossible) {
            if(r.nonTerminalTags != null) {
              resultValue.append(SpecialString.mainSeparator).append(r.nonTerminalTags)
            }
          } else if (ep.posType == POSType.Frame) {
            if (r.nonTerminalTags != null) {
              resultValue.append(SpecialString.mainSeparator).append("1\t").append(r.nonTerminalTags)
            }
          }
        }
        result(i) = (resultKey.toString(), resultValue.toString())
        i += 1
      }
      result
    }).reduceByKey((a, b)=> mergeFunction(a, b, ep))
      .mapPartitions(part => {
        part.map(entry => {
          val keySplit = entry._1.split(Splitter.mainSplitter)
          val valueSplit = entry._2.split(Splitter.mainSplitter)

          val newKey = new StringBuilder(64)
          val newValue = new StringBuilder(128)

          newKey.append(keySplit(0)).append(SpecialString.mainSeparator).append(keySplit(1))

          newValue.append(keySplit(2)).append(SpecialString.mainSeparator)
            .append(valueSplit(0)).append(SpecialString.mainSeparator).append(valueSplit(0))
          if (valueSplit.length > 1) {
            for (i <- 1 until valueSplit.length)
              newValue.append(SpecialString.mainSeparator).append(valueSplit(i))
          }
          (newKey.toString(), newValue.toString())
        })
      }).reduceByKey((a, b) => {
      val aSplit = a.split(Splitter.mainSplitter)
      val bSplit = b.split(Splitter.mainSplitter)
      val result = new StringBuilder(64)

      if (aSplit(1).toInt > bSplit(1).toInt)
        result.append(aSplit(0)).append(SpecialString.mainSeparator).append(aSplit(1)).append(SpecialString.mainSeparator).append(aSplit(2).toInt + bSplit(2).toInt)
      else
        result.append(bSplit(0)).append(SpecialString.mainSeparator).append(bSplit(1)).append(SpecialString.mainSeparator).append(aSplit(2).toInt + bSplit(2).toInt)

      val sb = new StringBuilder(128)
      val syntaxString: Object2FloatOpenHashMap[String] = new Object2FloatOpenHashMap[String]
      if (aSplit.length > 3) mergeSyntaxInfo(aSplit(3), syntaxString)
      if (bSplit.length > 3) mergeSyntaxInfo(bSplit(3), syntaxString)

      var POSInfo : ObjectArrayList[String] = null
      if(aSplit.length > 4 && bSplit.length > 4) {
        val posA : ObjectArrayList[String] = getPOSInfo(aSplit(4))
        val posB : ObjectArrayList[String] = getPOSInfo(bSplit(4))
        for (i <- 0 until posA.size()) {
          var init = posA.get(i)
          val tmp : Array[String] = posB.get(i).trim().split(" ")
          for (j <- tmp.indices)
            init = insertIntoStringSet(init, tmp(j))
          posA.set(i, init)
        }
        POSInfo = posA
      } else if (aSplit.length > 4 && bSplit.length < 4) POSInfo = getPOSInfo(aSplit(4))
        else if (aSplit.length < 2 && bSplit.length > 2) POSInfo = getPOSInfo(bSplit(2))

      var first: Boolean = true
      if (syntaxString != null) {
        sb.append(SpecialString.mainSeparator)
        val it = syntaxString.keySet().iterator()
        var key : String = ""

        while(it.hasNext) {
          if (first) first = false
          else sb.append(SpecialString.secondarySeparator)
          key = it.next()
          sb.append(syntaxString.getFloat(key)).append(SpecialString.fileSeparator).append(key)
        }
      }
      if (POSInfo != null) {
        sb.append(SpecialString.mainSeparator)
        first = true
        for (i <- 0 until POSInfo.size()) {
          if (first) first = false
          else sb.append(SpecialString.secondarySeparator)
          sb.append(POSInfo.get(i))
        }
      }
      if(sb.nonEmpty){
        result.append(sb.toString())
      }
      result.toString()
    }).mapPartitions(part => {
      part.map(entry => {
        val valueSplit = entry._2.split(Splitter.mainSplitter)
        val result = new StringBuilder(128)
        result.append(entry._1).append(SpecialString.mainSeparator).append(valueSplit(0)).append(SpecialString.mainSeparator).append(valueSplit(2))
        if (valueSplit.length > 3) {
          for (i <- 3 until valueSplit.length)
            result.append(SpecialString.mainSeparator).append(valueSplit(i))
        }
        result.toString()
      })
    }).persist(StorageLevel.MEMORY_ONLY)

    val res = sentence_library.count()
    println(sentence_library.first())
    println("Trigger action " + res)

    val GTCountPath = ep.rootDir + "/GTPath"
    var gts : Option[GTSmoothing] = None
    if (ep.allowGoodTuring || ep.allowKN || ep.allowMKN) {
      sentence_library.map(elem => (elem.split(Splitter.mainSplitter)(3), 1)).reduceByKey(_+_)
        .map(x => x._1 + " ," + x._2)
        .saveAsTextFile(GTCountPath)
    }
    if (ep.allowGoodTuring){
      val gtCount = sc.sparkContext.textFile(GTCountPath).map(line => (line.split(" ,")(0).toFloat , line.split(" ,")(1).toFloat))
      val r : FloatArrayList = new FloatArrayList()
      val Nr : FloatArrayList = new FloatArrayList()
      for (elem <- gtCount.collect()) {
        r.add(elem._1)
        Nr.add(elem._2)
      }
      gts = Some(new GTSmoothing(r, Nr))
    }
    val gtBroadCast = sc.sparkContext.broadcast(gts)

    val ruleCTotal = sentence_library.mapPartitions(part => part.map(entry => {
      val entrySplit = entry.split(Splitter.mainSplitter)
      (entrySplit(0), entrySplit(3) + "," + 0)
    })).reduceByKey((a, b) => {
      val aSplit = a.split(",").map(_.toFloat)
      val bSplit = b.split(",").map(_.toFloat)

      var GTCount : Float = aSplit(1) + bSplit(1)
      val count : Float = aSplit(0) + bSplit(0)
      if (ep.allowGoodTuring && gtBroadCast.value.isDefined) {
        GTCount += gtBroadCast.value.get.getSmoothedCount(aSplit(0)) + gtBroadCast.value.get.getSmoothedCount(aSplit(0))
      }
      count + "," + GTCount
    }).mapPartitions(part => part.map(elem => {
      val split = elem._2.split(",").map(_.toFloat)
      var result : Option[String] = None
      if (ep.allowGoodTuring) {
        result = Some(gtBroadCast.value.get.getSmoothedTotalCount(split(0), split(1)) + "")
      } else {
        result = Some(split(0) + "")
      }
      (elem._1, result.get)
    })).partitionBy(new HashPartitioner(ep.partitionNum))


    val ruleETotal = sentence_library.mapPartitions(part => part.map(entry => {
      val entrySplit = entry.split(Splitter.mainSplitter)
      (entrySplit(1), entrySplit(3) + "," + 0)
    })).reduceByKey((a, b) => {
      val aSplit = a.split(",").map(_.toFloat)
      val bSplit = b.split(",").map(_.toFloat)

      var GTCount : Float = aSplit(1) + bSplit(1)
      val count : Float = aSplit(0) + bSplit(0)
      if (ep.allowGoodTuring && gtBroadCast.value.isDefined) {
        GTCount += gtBroadCast.value.get.getSmoothedCount(aSplit(0)) + gtBroadCast.value.get.getSmoothedCount(aSplit(0))
      }
      count + "," + GTCount
    }).mapPartitions(part => {
      part.map(elem => {
        val split = elem._2.split(",").map(_.toFloat)
        var result : Option[String] = None
        if (ep.allowGoodTuring) {
          result = Some(gtBroadCast.value.get.getSmoothedTotalCount(split(0), split(1)) + "")
        } else {
          result = Some(split(0) + "")
        }
        (elem._1, result.get)
      })
    }).partitionBy(new HashPartitioner(ep.partitionNum))

    val epBroadcast = sc.sparkContext.broadcast(ep)

    val rule_combineTC = sentence_library.filter(_.split(Splitter.mainSplitter)(3).toInt > epBroadcast.value.cutoff)
      .mapPartitions(part => part.map(entry => (entry.split(Splitter.mainSplitter)(0), entry)), preserve)
      .partitionBy(new HashPartitioner(epBroadcast.value.partitionNum))
      .join(ruleCTotal)
      .mapPartitions(part => { part.map(entry => {
        val newValue = new StringBuilder(128)
        newValue.append(entry._2._1).append(SpecialString.mainSeparator).append(entry._2._2)
        newValue.toString()
      })
    }, preserve)
      .mapPartitions(part => { part.map(entry => (entry.split(Splitter.mainSplitter)(1), entry)) })
      .partitionBy(new HashPartitioner(ep.partitionNum))
      .join(ruleETotal).mapPartitions(part => {
      part.map(entry => {
        val newValue = new StringBuilder(128)
        newValue.append(entry._2._1).append(SpecialString.mainSeparator).append(entry._2._2)
        newValue.toString()
      })
    }, preserve)

    //(rhs, lhs ||| rhs ||| alignStr ||| totalCount ||| SyntaxInfo ||| posTagInfo ||| posFrameInfo ||| totalCountC)

    sentence_library.unpersist()
    rule_combineTC
  }
  
  def mergeFunction(A: String, B: String, ep: ExtractionParameters): String = {

    val aSplit = A.split(" \\|\\|\\| ")
    val bSplit = B.split(" \\|\\|\\| ")
    
    val key = new StringBuilder(64)
    key.append(aSplit(0).toInt + bSplit(0).toInt)
    
    val sb = new StringBuilder(128)
    val syntaxString : Object2FloatOpenHashMap[String] = new Object2FloatOpenHashMap[String]
    if (aSplit.length > 1) mergeSyntaxInfo(aSplit(1), syntaxString)
    if (bSplit.length > 1) mergeSyntaxInfo(bSplit(1), syntaxString)
    var POSInfo : ObjectArrayList[String] = null

    if(aSplit.length > 2 && bSplit.length > 2) {
      val posA : ObjectArrayList[String] = getPOSInfo(aSplit(2))
      val posB : ObjectArrayList[String] = getPOSInfo(bSplit(2))
      for (i <- 0 until posA.size()) {
        var init = posA.get(i)
        val tmp : Array[String] = posB.get(i).trim().split(" ")
        for (j <- tmp.indices)
          init = insertIntoStringSet(init, tmp(j))

        posA.set(i, init)
      }
      POSInfo = posA
    } else if (aSplit.length > 2 && bSplit.length < 2) POSInfo = getPOSInfo(aSplit(2))
      else if (aSplit.length < 2 && bSplit.length > 2) POSInfo = getPOSInfo(bSplit(2))

    var first: Boolean = true
    if (syntaxString != null) {
      sb.append(SpecialString.mainSeparator)
      val it = syntaxString.keySet().iterator()
      var key : String = ""

      while(it.hasNext) {
        if (first) first = false
        else sb.append(SpecialString.secondarySeparator)
        key = it.next()
        sb.append(syntaxString.getFloat(key)).append(SpecialString.fileSeparator).append(key)
      }
    }
    if (POSInfo != null) {
      sb.append(SpecialString.mainSeparator)
      first = true
      for (i <- 0 until POSInfo.size()) {
        if (first) first = false
        else sb.append(SpecialString.secondarySeparator)
        sb.append(POSInfo.get(i))
      }
    }
    
    if(sb.nonEmpty){
      key.append(sb.toString())
    }
    
    key.toString
  }
  def mergeSyntaxInfo(StrA: String, currentMap : Object2FloatOpenHashMap[String]) : Object2FloatOpenHashMap[String] = {
    val arrA = StrA.split(" \\|\\|\\|\\| ")
    for (elem <- arrA) {
      val index = elem.indexOf("\t")
      if (index > 0) {
        val count = elem.substring(0, elem.indexOf("\t")).toFloat
        val key = elem.substring(elem.indexOf("\t") + 1)
        if (currentMap.containsKey(key)) {
          currentMap.put(key, count + currentMap.getFloat(key))
        } else {
          currentMap.put(key, count)
        }
      }
    }
    currentMap
  }

  def getPOSInfo(str: String) : ObjectArrayList[String] = {
    val arrA = str.split(" \\|\\|\\|\\| ")
    val pos:ObjectArrayList[String] = new ObjectArrayList[String](arrA.length)

    for (i <- 0 until arrA.length; if i < arrA.length) {
      pos.add("")
    }
    for (i <- 0 until arrA.length) {
      pos.set(i, insertIntoStringSet(pos.get(i), arrA(i)))
    }
    pos
  }

  def insertIntoStringSet(theSet:String, items:String): String = {
    val itemList : Array[String] = items.trim().split(" ")
    val result : StringBuilder = theSet match {
      case _ if theSet != null => new StringBuilder(theSet)
      case _  => new StringBuilder("")
    }
    for (item <- itemList) {
      if (result.isEmpty)
        result.append(" " + item + " ")
      else if(!result.toString.contains(" " + item + " "))
        result.append(item + " ")
    }
    result.toString
  }
}
