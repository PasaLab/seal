package edu.nju.pasalab.mt.extraction.spark.exec

import java.util.concurrent.TimeUnit

import edu.nju.pasalab.mt.extraction.util.alignment.AlignmentTable
import edu.nju.pasalab.mt.extraction.util.smoothing.{MKNSmoothing, KNSmoothing, GTSmoothing}
import edu.nju.pasalab.mt.util.SntToCooc
import edu.nju.pasalab.util.ExtractionParameters
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.{LoggerFactory, Logger}

import edu.nju.pasalab.mt.extraction.Scoring
import edu.nju.pasalab.mt.extraction.dataStructure.{SpecialString, TranslationUnit, Splitter}

import org.apache.spark.rdd.RDD
import scala.collection._


object PhraseBasedModel {
  val logger:Logger  = LoggerFactory.getLogger(PhraseBasedModel.getClass)

  def computeLexicalPro(sc: SparkSession, ep:ExtractionParameters) {

    println("\nCompute Word translation probability table\n")
    val t1 = System.nanoTime()

    val wordPairs = WordTranslationPro.countWordPairs(sc, SntToCooc.getHDFSTMPath(ep.rootDir), ep.partitionNum)

    val wordTranslationC2E = WordTranslationPro.computeWordProbC2E(wordPairs)
    val probC2EMap = WordTranslationPro.saveWordProInMap(wordTranslationC2E)

    val wordTranslationE2C = WordTranslationPro.computeWordProbE2C(wordPairs)
    val probE2CMap = WordTranslationPro.saveWordProInMap(wordTranslationE2C)

    val T1 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS) / 1000.0

    println("\nCompute Word translation probability table waste " + T1 + " .s\n")

    val probC2EBroadcast = sc.sparkContext.broadcast(probC2EMap)
    val probE2CBroadcast = sc.sparkContext.broadcast(probE2CMap)

    val epBroadcast = sc.sparkContext.broadcast(ep)
    val t2 = System.nanoTime()

    //phrase_combineTE: phraseC ||| phraseE ||| alignStr ||| countPair ||| syntaxInfo ||| posTagInfo ||| posFrameInfo ||| totalCountC ||| totalCountE)
    //                     0           1          2             3          (if exists)     ...             ...            length-2       length-1

    val phraseTE = getRDD(PhraseExtraction.extract_phrase(sc, ep), ep, t2).persist(StorageLevel.MEMORY_ONLY)

    val res = phraseTE.count()

    println("Trigger action " + res)

    var t = System.nanoTime()
    val GTCountPath = ep.rootDir + "/GTPath"
    var gts : Option[GTSmoothing] = None
    if (ep.allowGoodTuring){
      t = System.nanoTime()

      val gtCount = sc.sparkContext.textFile(GTCountPath).map(line => (line.split(" ,")(0).toFloat , line.split(" ,")(1).toFloat))
      val r : FloatArrayList = new FloatArrayList()
      val Nr : FloatArrayList = new FloatArrayList()

      for (elem <- gtCount.collect()) {
        r.add(elem._1)
        Nr.add(elem._2)
      }
      gts = Some(new GTSmoothing(r, Nr))

      val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t, TimeUnit.NANOSECONDS) / 1000.0
      println("Get Good Turing Para waste " + T +  " . s\n")
    }
    val gtBroadCast = sc.sparkContext.broadcast(gts)
    
    val KNPath = ep.rootDir + "/KNPath"
    var KNS : Option[KNSmoothing] = None
    if (ep.allowKN) {

      t = System.nanoTime()
      val gtCount = sc.sparkContext.textFile(GTCountPath).map(line => (line.split(" ,")(0).toFloat , line.split(" ,")(1).toFloat))
      val n1 = gtCount.filter(_._1 == 1f).map(x => x._2).collect()(0)
      val n2 = gtCount.filter(_._1 == 2f).map(x => x._2).collect()(0)

      val KNSum = sc.sparkContext.textFile(KNPath).map(line => {
        val fields = line.split("\t")
        (fields(0), fields(2).toFloat)
      }).reduceByKey(_+_)

      val allCountC = KNSum.filter(_._1.contains("C2E")).map(x => x._2).collect()(0)
      val allCountE = KNSum.filter(_._1.contains("E2C")).map(x => x._2).collect()(0)
      KNS = Some(new KNSmoothing(n1, n2, allCountC, allCountE))

      val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t, TimeUnit.NANOSECONDS) / 1000.0
      println("Get KN Para waste " + T +  " . s\n")
    }
    val KNBroadCast = sc.sparkContext.broadcast(KNS)

    var MKNS : Option[MKNSmoothing] = None
    if (ep.allowMKN) {
      t = System.nanoTime()

      val gtCount =  sc.sparkContext.textFile(GTCountPath).map(line => (line.split(" ,")(0).toFloat , line.split(" ,")(1).toFloat))
      val n1 = gtCount.filter(_._1 == 1f).map(x => x._2).collect()(0)
      val n2 = gtCount.filter(_._1 == 2f).map(x => x._2).collect()(0)
      val n3 = gtCount.filter(_._1 == 3f).map(x => x._2).collect()(0)
      val n4 = gtCount.filter(_._1 == 4f).map(x => x._2).collect()(0)
      val KNSum = sc.sparkContext.textFile(KNPath).map(line => {
        val fields = line.split("\t")
        (fields(0), fields(2).toFloat)
      }).reduceByKey(_+_)

      val allCount = KNSum.filter(_._1.contains("C2E")).map(x => x._2).collect()(0)
      MKNS = Some(new MKNSmoothing(n1, n2, n3, n4, allCount))
      val T = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t, TimeUnit.NANOSECONDS) / 1000.0
      println("Get MKN Para waste " + T +  " . s\n")
    }
    val MKNBroadCast = sc.sparkContext.broadcast(MKNS)
    println("Done Smoothing para")

    val t3 = System.nanoTime()
    println("\nlast step, get lexicalize weight")


    val src = sc.sparkContext.textFile(SntToCooc.getCorpusSrcDict(ep.rootDir)).filter(_.length > 0).map(line => {
      val sp = line.trim.split("\\s+")
      (sp(0), sp(1).toInt)
    })
    val tgt =  sc.sparkContext.textFile(SntToCooc.getCorpusTgtDict(ep.rootDir)).filter(_.length > 0).map(line => {
      val sp = line.trim.split("\\s+")
      (sp(0), sp(1).toInt)
    })
    val srcD : Int2ObjectOpenHashMap[String] = new Int2ObjectOpenHashMap[String]
    for (elem <- src.collect()) {
      srcD.put(elem._2, elem._1)
    }
    val tgtD : Int2ObjectOpenHashMap[String] = new Int2ObjectOpenHashMap[String]
    for (elem <- tgt.collect()) {
      tgtD.put(elem._2, elem._1)
    }
    val srcDB = sc.sparkContext.broadcast(srcD)
    val tgtDB = sc.sparkContext.broadcast(tgtD)

/*    val reorderTable = sc.sparkContext.textFile(ep.rootDir + "/reorderTablePath").map(elem => {
      val sp = elem.split(Splitter.mainSplitter)
      val key = new StringBuilder
      key.append(sp(0))
      key.append(SpecialString.mainSeparator)
      key.append(sp(1))
      (key.toString(), sp(2))
    })*/

    phraseTE.mapPartitions(part => fun(part, epBroadcast, probC2EBroadcast, probE2CBroadcast, srcDB, tgtDB, gtBroadCast, KNBroadCast, MKNBroadCast))
      .repartition(ep.totalCores)
      .saveAsTextFile(ep.rootDir + "/combinedResultC2E")

/*      .join(reorderTable)
      .mapPartitions(part => {
        val srcDict = srcDB.value
        val tgtDict = tgtDB.value
        part.map(elem => {
          val sb = new StringBuilder
          val sp = elem._1.split(Splitter.mainSplitter)
          for (e <- sp(0).trim.split("\\s+"))
            sb.append(srcDict.get(e.toInt)).append(" ")
          sb.append(SpecialString.mainSeparator)
          for (e <- sp(1).trim.split("\\s+"))
            sb.append(tgtDict.get(e.toInt)).append(" ")

          val value = elem._2._1.split(Splitter.mainSplitter)
          sb.append(SpecialString.mainSeparator).append(value(0)).append(SpecialString.mainSeparator).append(value(1))
            .append("\t").append(elem._2._2)

          for (i <- 2 until value.length if epBroadcast.value.withSyntaxInfo)
            sb.append(SpecialString.mainSeparator).append(value(i))
          sb.toString()
        })
      }).repartition(ep.totalCores)
      .saveAsTextFile(ep.rootDir + "/combinedResultC2E")*/


    val T3 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t3, TimeUnit.NANOSECONDS) / 1000.0
    println("last step, get lexicalScore weight, waste time " + T3 + " . s\n")

  }

  def fun(part : Iterator[String], epBroadcast :  Broadcast[ExtractionParameters],
          probC2EBroadcast : Broadcast[mutable.HashMap[String, Object2FloatOpenHashMap[String]]] ,
          probE2CBroadcast : Broadcast[mutable.HashMap[String, Object2FloatOpenHashMap[String]]],
          srcDB : Broadcast[Int2ObjectOpenHashMap[String]],
          tgtDB : Broadcast[Int2ObjectOpenHashMap[String]],
          gtBroadCast :  Broadcast[Option[GTSmoothing]] ,
          KNBroadCast : Broadcast[Option[KNSmoothing]],
          MKNBroadCast : Broadcast[Option[MKNSmoothing]]) : Iterator[String] = {


    val probC2E = probC2EBroadcast.value
    val probE2C = probE2CBroadcast.value
    val srcDict = srcDB.value
    val tgtDict = tgtDB.value

    var s: Option[Scoring] = None
    if (epBroadcast.value.allowGoodTuring) {

      s = Some(new Scoring(probC2E, probE2C, gtBroadCast.value.get, epBroadcast.value))
    } else if (epBroadcast.value.allowKN) {

      s = Some(new Scoring(probC2E, probE2C, KNBroadCast.value.get, epBroadcast.value))
    } else if (epBroadcast.value.allowMKN) {

      s = Some(new Scoring(probC2E, probE2C, MKNBroadCast.value.get, epBroadcast.value))
    } else {
      s = Some(new Scoring(probC2E, probE2C, epBroadcast.value))
  }

    part.map(entry => {
      val line = entry.split(Splitter.mainSplitter)
      var aUnit: Option[TranslationUnit] = None
      if (epBroadcast.value.allowKN) {
        val sb = new StringBuilder(128)
        sb.append(line(0))
        for (i <- 1 until line.length - 4)
          sb.append(SpecialString.mainSeparator).append(line(i))
        aUnit = Some(new TranslationUnit(sb.toString(), epBroadcast.value))

        aUnit.get.totalCount = line(line.length - 4).toFloat //Tc
        aUnit.get.n1plusC = Some(line(line.length - 2).toFloat) //Nc
        aUnit.get.n1plusE = Some(line(line.length - 1).toFloat) //Ne
      } else if(epBroadcast.value.allowMKN) {

        val sb = new StringBuilder(128)
        sb.append(line(0))
        for (i <- 1 until line.length - 8)
          sb.append(SpecialString.mainSeparator).append(line(i))

        aUnit = Some(new TranslationUnit(sb.toString(), epBroadcast.value))
        aUnit.get.n1C = Some(line(line.length - 6).toFloat) //n1C
        aUnit.get.n2C = Some(line(line.length - 5).toFloat) //n2C
        aUnit.get.n3plusC = Some(line(line.length - 4).toFloat) //n3plusC
        aUnit.get.n1plusE = Some(line(line.length - 1).toFloat + line(line.length - 2).toFloat + line(line.length - 3).toFloat) //n2plusE + n2E + n1E
        aUnit.get.totalCount = line(line.length - 8).toFloat //Tc
      } else {
        val totalCountC = line(line.length - 2).toFloat
        val unitStrC2E = new StringBuilder(128)
        unitStrC2E.append(line(0))
        for (i <- 1 until line.length - 2) {
          unitStrC2E.append(SpecialString.mainSeparator + line(i))
        }
        aUnit = Some(new TranslationUnit(unitStrC2E.toString(), epBroadcast.value))
        aUnit.get.totalCount = totalCountC
      }

      val c2eResult = s.get.calculateTransUnitScoreOnOneUnit(aUnit.get)

      var bUnit : Option[TranslationUnit] = None

      val unitStr = new StringBuilder(128)
      unitStr.append(line(1) + SpecialString.mainSeparator + line(0) + SpecialString.mainSeparator
        + AlignmentTable.getReverseAlignStr(line(2)) + SpecialString.mainSeparator + line(3))

      if (epBroadcast.value.allowKN) {
        val sb = new StringBuilder(128)
        sb.append(unitStr)
        for (i <- 4 until line.length - 4)
          sb.append(SpecialString.mainSeparator).append(line(i))

        bUnit = Some(new TranslationUnit(sb.toString(), epBroadcast.value))
        bUnit.get.n1plusC = Some(line(line.length - 1).toFloat) //Ne
        bUnit.get.n1plusE = Some(line(line.length - 2).toFloat) //Nc
        bUnit.get.totalCount = line(line.length - 3).toFloat    //Tc

      } else if( epBroadcast.value.allowMKN) {
        val sb = new StringBuilder(128)
        sb.append(unitStr)
        for (i <- 4 until line.length - 8)
          sb.append(SpecialString.mainSeparator).append(line(i))

        bUnit = Some(new TranslationUnit(sb.toString(), epBroadcast.value))
        bUnit.get.n1C = Some(line(line.length - 3).toFloat) //n1E
        bUnit.get.n2C = Some(line(line.length - 2).toFloat) //n2E
        bUnit.get.n3plusC = Some(line(line.length - 1).toFloat) //n3plusE
        bUnit.get.n1plusE = Some(line(line.length - 4).toFloat + line(line.length - 5).toFloat + line(line.length - 6).toFloat) //n3plusC + n2C + n1C
        bUnit.get.totalCount = line(line.length - 7).toFloat //Tc

      } else {
        if (line.length > 6) {
          for (i <- 4 until line.length - 2)
            unitStr.append(SpecialString.mainSeparator + line(i))
        }

        val totalCountE = line(line.length - 1).toFloat
        bUnit = Some(new TranslationUnit(unitStr.toString(), epBroadcast.value))
        bUnit.get.totalCount = totalCountE
      }

      val e2cResult = s.get.calculateReverseTransUnitScoreOnOneUnit(bUnit.get)

      c2eResult.printCombineWeighting(e2cResult.relativeFreq, e2cResult.lexicalScore, epBroadcast.value.withSyntaxInfo, srcDict, tgtDict)
      //phraseC ||| phraseE ||| alignStr ||| relativeFreqC lexicalFreqC  relativeFreqE lexicalFreqE ||| syntaxInfo ||| posTagInfo ||| posFrameInfo
    })
  }

  def getRDD(phrase_combineTE : RDD[String], ep:ExtractionParameters, t: Long): RDD[String] = {

    val T2 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t, TimeUnit.NANOSECONDS) / 1000.0
    println("\nExtract Phrase " + T2 + " .s\n")

    println("\npreprocess smoothing KN & MKN")
    val t3 = System.nanoTime()
    var result_phrase : RDD[String] = null

    val KNPath = ep.rootDir + "/KNPath"

    if (ep.allowKN) {
      val KNCount = phrase_combineTE.map(line => {
        val fields = line.split(Splitter.mainSplitter)
        val array : Array[(String, Int)] = new Array[(String, Int)](2)
        array(0) = ("C2E" + "\t" + fields(0), 1)
        array(1) = ("E2C" + "\t" + fields(1), 1)
        array
      }).flatMap(x => x).reduceByKey(_+_)

      KNCount.map(x => x._1 + "\t" + x._2).saveAsTextFile(KNPath)

      val KN_PhraseC = KNCount.filter(line => line._1.contains("C2E")).map(line => (line._1.split("\t")(1), line._2))
      val KN_PhraseE = KNCount.filter(line => line._1.contains("E2C")).map(line => (line._1.split("\t")(1), line._2))

      val KN_Step1 = phrase_combineTE.map(line => {
        val split = line.split(Splitter.mainSplitter)
        (split(0), line)
      }).join(KN_PhraseC).map(elem =>{
        val split = elem._2._1.split(Splitter.mainSplitter)
        val value = new StringBuilder(64)
        value.append(elem._2._1).append(SpecialString.mainSeparator).append(elem._2._2)
        (split(1), value.toString())
      })

      result_phrase = KN_Step1.join(KN_PhraseE).map(elem => {
        val value = new StringBuilder(64)
        value.append(elem._2._1).append(SpecialString.mainSeparator).append(elem._2._2)
        value.toString()
      })

    } else if (ep.allowMKN) {
      val MKNCount = phrase_combineTE.map(line => {
        val split = line.split(Splitter.mainSplitter)
        val array : Array[(String, Int)] = new Array[(String, Int)](2)
        val count = split(3).toFloat
        if (count == 1f) {
          array(0) = ("C2E" + "\t" + split(0) + "\t" + 1, 1)
          array(1) = ("E2C" + "\t" + split(1) + "\t" + 1, 1)
        } else if (count == 2f) {
          array(0) = ("C2E" + "\t" + split(0) + "\t" + 2, 1)
          array(1) = ("E2C" + "\t" + split(1) + "\t" + 2, 1)
        } else {
          array(0) = ("C2E" + "\t" + split(0) + "\t" + 3, 1)
          array(1) = ("E2C" + "\t" + split(1) + "\t" + 3, 1)
        }
        array
      }).flatMap(x => x).reduceByKey(_+_)

      MKNCount.map(x => x._1 + "\t" + x._2).saveAsTextFile(KNPath)

      val MKN_PhraseC = MKNCount.filter(line => line._1.contains("C2E"))
      .map(elem => (elem._1.split("\t")(1), elem._1.split("\t")(2) + "\t" + elem._2))
      .groupByKey().map(elem => {
        val value = new StringBuilder(64)
        var n1 : Option[String] = None
        var n2 : Option[String] = None
        var n3 : Option[String] = None
        for (e <- elem._2) {
          val tmp = e.split("\t")
          tmp(0).toInt match {
            case 1 => n1 = Some(tmp(1))
            case 2 => n2 = Some(tmp(1))
            case _ => n3 = Some(tmp(1))
          }
        }
        value.append(n1.getOrElse("0")).append(SpecialString.mainSeparator).append(n2.getOrElse("0"))
        .append(SpecialString.mainSeparator).append(n3.getOrElse("0"))
        (elem._1 , value.toString()) 
      })
      
      
      val MKN_PhraseE = MKNCount.filter(line => line._1.contains("E2C"))
      .map(elem => (elem._1.split("\t")(1), elem._1.split("\t")(2) + "\t" + elem._2))
      .groupByKey().map(elem => {
        val value = new StringBuilder(64)
        var n1 : Option[String] = None
        var n2 : Option[String] = None
        var n3 : Option[String] = None
        for (e <- elem._2) {
          val tmp = e.split("\t")
          tmp(0).toInt match {
            case 1 => n1 = Some(tmp(1))
            case 2 => n2 = Some(tmp(1))
            case _ => n3 = Some(tmp(1))
          }
        }
        value.append(n1.getOrElse("0")).append(SpecialString.mainSeparator).append(n2.getOrElse("0"))
        .append(SpecialString.mainSeparator).append(n3.getOrElse("0"))
        (elem._1 , value.toString()) 
      })

      val MKN_Step1 =  phrase_combineTE.map(line => {
        val split = line.split(Splitter.mainSplitter)
        (split(0), line)
      }).join(MKN_PhraseC).map(elem => {
        val split = elem._2._1.split(Splitter.mainSplitter)
        val value = new StringBuilder(64)
        value.append(elem._2._1).append(SpecialString.mainSeparator).append(elem._2._2)
        (split(1), value.toString())
      })

      result_phrase = MKN_Step1.join(MKN_PhraseE).map(elem => {
        val value = new StringBuilder(64)
        value.append(elem._2._1).append(SpecialString.mainSeparator).append(elem._2._2)
        value.toString()
      })
    } else {
      result_phrase = phrase_combineTE
    }

    val T3 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t3, TimeUnit.NANOSECONDS) / 1000.0
    println("Done preprocess smoothing KN & MKN, waste time " + T3 + " . s\n")

    result_phrase
  }
}
