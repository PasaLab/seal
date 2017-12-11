package edu.nju.pasalab.mt.extraction.spark.exec

import java.io.{OutputStreamWriter, BufferedWriter, PrintWriter}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import edu.nju.pasalab.mt.extraction.util.smoothing.GTSmoothing
import edu.nju.pasalab.mt.util.{CommonFileOperations, SntToCooc}
import edu.nju.pasalab.util._
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.objects.{Object2IntOpenHashMap, ObjectArrayList, Object2FloatOpenHashMap}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.slf4j.{LoggerFactory, Logger}
import edu.nju.pasalab.mt.extraction.util.dependency.DependencyTree
import edu.nju.pasalab.mt.extraction.{Reordering,  ExtractionFromAlignedSentence}
import edu.nju.pasalab.mt.extraction.dataStructure._
import edu.nju.pasalab.mt.extraction.util.alignment.AlignedSentence
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import edu.nju.pasalab.mt.optimize.join.optimizeJoin._

object PhraseExtraction {
  val preserve = true
  val logger:Logger  = LoggerFactory.getLogger(PhraseExtraction.getClass)
  val empty:Pattern = Pattern.compile("^\\s*$")

  def extract_phrase(sc: SparkSession, ep: ExtractionParameters):RDD[String] ={

    val sentence_library = sc.sparkContext.textFile(SntToCooc.getHDFSTMPath(ep.rootDir), ep.partitionNum)
      .flatMap({ sent_pair => {
      val content = sent_pair.split(Splitter.mainSplitter)
      val as = new AlignedSentence(content(0).trim, content(1).trim, content(2).trim)

      var dt:DependencyTree = null
      var pos:Array[String] = null
      if (content.length > 3) {

        if (ep.syntax == SyntaxType.S2D){
          if (!((content.length < 4 || empty.matcher(content(3)).matches()) && ep.constrainedType != ConstrainedType.ANY)) {

            val dep:Array[Int] = content(3).trim.split(" ").map(elem => elem.toInt)
            dt = new DependencyTree(as.trgSentence.wordSequence, dep)
            if (ep.posType != POSType.None) {
              if(!((content.length < 5 || empty.matcher(content(4)).matches()) && ep.constrainedType != ConstrainedType.ANY))
                pos = content(4).trim.split("\\s+")
            }
          }
        }
      }
      val e = new ExtractionFromAlignedSentence(ep)
      e.extractSentence(as, dt, pos)
        val list = e.getPhraseList()
        val result: Array[(String, String)] = new Array[(String, String)](list.length)
        for (i <- list.indices) {

          val resultKey = new StringBuilder(64)
          resultKey.append(list(i).phraseC).append(SpecialString.mainSeparator).append(list(i).phraseE).append(SpecialString.mainSeparator)
          .append(list(i).alignStr)

          val resultValue = new StringBuilder(128)
          resultValue.append(list(i).reorderInfo).append(SpecialString.mainSeparator).append(1)

          if (content.length > 3) {
            if (ep.depType == DepType.DepString) {
              resultValue.append(SpecialString.mainSeparator).append("1\t").append(list(i).syntaxString)
            }
            if (ep.posType == POSType.AllPossible) {
              if (list(i).posTagString != null){
                resultValue.append(SpecialString.mainSeparator).append(list(i).posTagString)
              }
            } else if (ep.posType == POSType.Frame) {
              if (list(i).posTagString != null){
                resultValue.append(SpecialString.mainSeparator).append("1\t").append(list(i).posTagString)
              }
            }
          }
          result(i) = (resultKey.toString(), resultValue.toString())
        }
        //key:   PhraseC ||| phraseE ||| alignStr
        //value: reorderInfo ||| count ||| syntaxStr ||| postagStr ||| posFrame
      result
    }})
      .reduceByKey((a, b)=> mergeFunction(a, b, ep))
      .mapPartitions(part => {
        part.map(entry => {
        val keySplit = entry._1.split(Splitter.mainSplitter)
        val valueSplit = entry._2.split(Splitter.mainSplitter)
        val newKey = new StringBuilder(64)
        val newValue = new StringBuilder(64)

        newKey.append(keySplit(0)).append(SpecialString.mainSeparator).append(keySplit(1))

        newValue.append(keySplit(2)).append(SpecialString.mainSeparator).append(Reordering.parseOrder(valueSplit(0)).mkString(" "))
          .append(SpecialString.mainSeparator).append(valueSplit(1)).append(SpecialString.mainSeparator).append(valueSplit(1))
        if (valueSplit.length > 2) {
          for (i <- 2 until valueSplit.length)
            newValue.append(SpecialString.mainSeparator).append(valueSplit(i))
        }
          //value : alignStr ||| reorderInfo ||| totalCount ||| totalCount |||   ...
        (newKey.toString(), newValue.toString())
      })
    }).reduceByKey((a, b) => {
      val aSplit = a.split(Splitter.mainSplitter)
      val bSplit = b.split(Splitter.mainSplitter)

      val key = new StringBuilder(64)
      val reorder = Reordering.combineOrder(aSplit(1),bSplit(1))
      aSplit(2).toInt > bSplit(2).toInt match {
        case true => key.append(aSplit(0)).append(SpecialString.mainSeparator).append(reorder).append(SpecialString.mainSeparator)
          .append(aSplit(2)).append(SpecialString.mainSeparator).append(aSplit(3).toInt + bSplit(3).toInt)
        case false => key.append(bSplit(0)).append(SpecialString.mainSeparator).append(reorder).append(SpecialString.mainSeparator)
          .append(bSplit(2)).append(SpecialString.mainSeparator).append(aSplit(3).toInt + bSplit(3).toInt)
      }

      //value : alignStr ||| reorderInfo ||| maxCount ||| totalCount |||   ...

      val sb = new StringBuilder(128)
      val syntaxString: Object2FloatOpenHashMap[String] = new Object2FloatOpenHashMap[String]
      if (aSplit.length > 4) mergeSyntaxInfo(aSplit(4), syntaxString)
      if (bSplit.length > 4) mergeSyntaxInfo(bSplit(4), syntaxString)

      var POSInfo : ObjectArrayList[String] = null

      if(aSplit.length > 3 && bSplit.length > 3) {
        val posA : ObjectArrayList[String] = getPOSInfo(aSplit(3))
        val posB : ObjectArrayList[String] = getPOSInfo(bSplit(3))
        for (i <- 0 until posA.size()) {
          var init = posA.get(i)
          val tmp : Array[String] = posB.get(i).trim().split(" ")
          for (j <- tmp.indices)
            init = insertIntoStringSet(init, tmp(j))

          posA.set(i, init)
        }
        POSInfo = posA
      }
      else if (aSplit.length > 5 && bSplit.length < 5) POSInfo = getPOSInfo(aSplit(5))
      else if (aSplit.length < 5 && bSplit.length > 5) POSInfo = getPOSInfo(bSplit(5))

      var first : Boolean = true
      if (syntaxString != null && syntaxString.size() > 0) {
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
      if (POSInfo != null && POSInfo.size() > 0) {
        sb.append(SpecialString.mainSeparator)
        first = true

        for (i <- 0 until POSInfo.size()) {
          if (first) first = false
          else sb.append(SpecialString.secondarySeparator)
          sb.append(POSInfo.get(i))
        }
      }
      if (sb.nonEmpty) {
        key.append(sb.toString())
      }
      key.toString()
    }).mapPartitions(part => {
      part.map(entry => {
        //value : alignStr ||| reorderInfo ||| totalCount |||   ...
        val valueSplit = entry._2.split(Splitter.mainSplitter)
        val result = new StringBuilder(128)
        result.append(entry._1).append(SpecialString.mainSeparator)
          .append(valueSplit(0)).append(SpecialString.mainSeparator).append(valueSplit(1)).append(SpecialString.mainSeparator).append(valueSplit(3))
        if (valueSplit.length > 4) {
          for (i <- 4 until valueSplit.length)
            result.append(SpecialString.mainSeparator).append(valueSplit(i))
        }
        result.toString()
      })
    }).distinct()

    //phraseC ||| phraseE ||| alignStr ||| reorderInfo ||| count ||| ....
/*    sentence_library.map(entry=>{
      val entrySplit = entry.split(Splitter.mainSplitter)
      val reorder = Reordering.formatOrder(entrySplit(3))
      val resultBuf = new StringBuilder
      resultBuf.append(entrySplit(0))
        .append(SpecialString.mainSeparator).append(entrySplit(1))
        .append(SpecialString.mainSeparator).append(reorder)
      resultBuf.toString()
    }).repartition(ep.totalCores).saveAsTextFile(ep.rootDir + "/reorderTablePath")*/

    val GTCountPath = ep.rootDir + "/GTPath"
    var gts : Option[GTSmoothing] = None
    if (ep.allowGoodTuring || ep.allowKN || ep.allowMKN) {
      sentence_library.map(elem => (elem.split(Splitter.mainSplitter)(4), 1))
        .reduceByKey(_+_)
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

    //phraseC ||| phraseE ||| alignStr ||| reorderInfo ||| count ||| ....
    val phraseTable = sentence_library.map(entry => {
      val sp = entry.split(Splitter.mainSplitter)
      val sb = new StringBuilder(128)
      sb.append(sp(0))
        .append(SpecialString.mainSeparator).append(sp(1))
        .append(SpecialString.mainSeparator).append(sp(2))
        .append(SpecialString.mainSeparator).append(sp(4))
      for (i <- 5 until sp.length)
        sb.append(SpecialString.mainSeparator).append(sp(i))
      sb.toString()
    }).persist()

    println(phraseTable.first())

    val phraseCTotal = phraseTable.map(entry => {
      val entrySplit = entry.split(Splitter.mainSplitter)
      (entrySplit(0), entrySplit(3) + "," + 0)
    }).reduceByKey((a, b) => {
      val aSplit = a.split(",").map(_.toFloat)
      val bSplit = b.split(",").map(_.toFloat)

      val count: Float = aSplit(0) + bSplit(0)
      var GTCount : Float = aSplit(1) + bSplit(1)
      if (ep.allowGoodTuring && gtBroadCast.value.isDefined) {
        GTCount += gtBroadCast.value.get.getSmoothedCount(aSplit(0)) + gtBroadCast.value.get.getSmoothedCount(bSplit(0))
      }
      count + "," + GTCount
    }).mapPartitions(part => {
      part.map(elem => {
        val split = elem._2.split(",").map(_.toFloat)
        var result : Option[Float] = None
        if(ep.allowGoodTuring) {
          result = Some(gtBroadCast.value.get.getSmoothedTotalCount(split(0), split(1)))
        } else {
          result = Some(split(0))
        }
        (elem._1, result.get)
      })
    }).partitionBy(new HashPartitioner(ep.parallelism))
      //.partitionBy(new HashPartitioner(ep.partitionNum))

    val phraseETotal = phraseTable.map(entry => {
      val entrySplit = entry.split(Splitter.mainSplitter)
      (entrySplit(1), entrySplit(3) + "," + 0)
    }).reduceByKey((a, b) => {
      val aSplit = a.split(",").map(_.toFloat)
      val bSplit = b.split(",").map(_.toFloat)

      val count: Float = aSplit(0) + bSplit(0)
      var GTCount : Float = aSplit(1) + bSplit(1)
      if (ep.allowGoodTuring && gtBroadCast.value.isDefined) {
        GTCount += gtBroadCast.value.get.getSmoothedCount(aSplit(0)) + gtBroadCast.value.get.getSmoothedCount(aSplit(0))
      }
      count + "," + GTCount
    }).mapPartitions(part => {
      part.map(elem => {
        val split = elem._2.split(",").map(_.toFloat)
        var result : Option[Float] = None
        if(ep.allowGoodTuring) {
          result = Some(gtBroadCast.value.get.getSmoothedTotalCount(split(0), split(1)))
        } else {
          result = Some(split(0))
        }
        (elem._1, result.get)
      })
    }).partitionBy(new HashPartitioner(ep.parallelism))

    println(phraseCTotal.take(1) + "\t" + phraseETotal.take(1))

    val phrase_combineTC = phraseTable.mapPartitions(part => { part.map(entry => (entry.split(Splitter.mainSplitter)(0), entry)) })
      .partitionBy(new HashPartitioner(ep.parallelism))
      //.leanJoin(phraseCTotal)
      .join(phraseCTotal)
      .mapPartitions(part => {
        part.map(entry => {
          val newValue = new StringBuilder(128)
          newValue.append(entry._2._1).append(SpecialString.mainSeparator).append(entry._2._2)
          newValue.toString()
        })
      }, preserve).mapPartitions(part => { part.map(entry => (entry.split(Splitter.mainSplitter)(1), entry)) })
      .partitionBy(new HashPartitioner(ep.parallelism))
      //.leanJoin(phraseETotal)
      .join(phraseETotal)
      .mapPartitions(part => {
        part.map(entry => {
          val newValue = new StringBuilder(128)
          newValue.append(entry._2._1).append(SpecialString.mainSeparator).append(entry._2._2)
          newValue.toString()
        })
      }, preserve)

    //phrase_combineTE: phraseC ||| phraseE ||| alignStr ||| countPair ||| syntaxInfo ||| posTagInfo ||| posFrameInfo ||| totalCountC ||| totalCountE)
    //                     0           1             2            3          (if exists)     ...             ...            length-2       length-1
    phraseTable.unpersist()
    phrase_combineTC
  }

  def mergeFunction(A:String, B:String, ep: ExtractionParameters): String = {
    //key:   PhraseC ||| phraseE ||| alignStr
    //value: reorderInfo ||| count ||| syntaxStr ||| postagStr ||| posFrame
    val aSplit = A.split(" \\|\\|\\| ")
    val bSplit = B.split(" \\|\\|\\| ")
    val key = new StringBuilder(64)
    key.append(Reordering.combineOrder(aSplit(0),bSplit(0))).append(SpecialString.mainSeparator).append(aSplit(1).toInt + bSplit(1).toInt)

    val sb = new StringBuilder(128)
    val syntaxString : Object2FloatOpenHashMap[String] = new Object2FloatOpenHashMap[String]

    if (aSplit.length > 2) mergeSyntaxInfo(aSplit(2), syntaxString)
    if (bSplit.length > 2) mergeSyntaxInfo(bSplit(2), syntaxString)

    var POSInfo : ObjectArrayList[String] = null

    if(aSplit.length > 3 && bSplit.length > 3) {
      val posA : ObjectArrayList[String] = getPOSInfo(aSplit(3))
      val posB : ObjectArrayList[String] = getPOSInfo(bSplit(3))

      for (i <- 0 until posA.size()) {
        var init = posA.get(i)
        val tmp : Array[String] = posB.get(i).trim().split(" ")
        for (j <- tmp.indices)
          init = insertIntoStringSet(init, tmp(j))

        posA.set(i, init)
      }
      POSInfo = posA
    } else if (aSplit.length > 3 && bSplit.length < 3) POSInfo = getPOSInfo(aSplit(3))
      else if (aSplit.length < 3 && bSplit.length > 3) POSInfo = getPOSInfo(bSplit(3))

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
    //first = true
    if (POSInfo != null) {
      sb.append(SpecialString.mainSeparator)
      first = true
      for (i <- 0 until POSInfo.size()) {
        if (first) first = false
        else sb.append(SpecialString.secondarySeparator)
        sb.append(POSInfo.get(i))
      }
    }
    if (sb.nonEmpty) {
      key.append(sb.toString())
    }
    key.toString()
  }

  def mergeSyntaxInfo(StrA: String, currentMap : Object2FloatOpenHashMap[String]) : Object2FloatOpenHashMap[String] = {
    val arrA = StrA.split(" \\|\\|\\|\\| ")
    for (elem <- arrA) {
      val index = elem.indexOf("\t")
      if (index > 0) {
        val count = elem.substring(0, index).toFloat
        val key = elem.substring(index + 1)
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
    for (i <- 0 until arrA.length; if i < arrA.length) {
      pos.set(i, insertIntoStringSet(pos.get(i), arrA(i)))
    }
    pos
  }

  def insertIntoStringSet(theSet:String, items : String): String = {
    val itemList : Array[String] = items.trim().split(" ")
    val result: StringBuilder = theSet match {
      case _ if !theSet.isEmpty => new StringBuilder(theSet)
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
}
