package edu.nju.pasalab.mt.extraction.spark.exec

import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap
import org.apache.spark.sql.SparkSession

import scala.collection._
import edu.nju.pasalab.mt.extraction.dataStructure.{Splitter, SpecialString}
import edu.nju.pasalab.mt.extraction.util.alignment.AlignmentTable
import org.apache.spark.rdd.RDD

/*
class WordTranslationPro {
  var countSrc: TObjectIntHashMap[String] = null
  var countTrg: TObjectIntHashMap[String] = null
  var countPair: scala.collection.mutable.HashMap[String, TObjectIntHashMap[String]] = null
  var probC2E: scala.collection.mutable.HashMap[String, TObjectFloatHashMap[String]] = null
  var probE2C: scala.collection.mutable.HashMap[String, TObjectFloatHashMap[String]] = null
  var totalSrcWords = 0
  var totalTrgWords = 0
  var totalSrcNull = 0
  var totalTrgNull = 0

  def trainCooccurence(sc: SparkSession, sentenceFileName: String) {
    countPair = new scala.collection.mutable.HashMap[String, TObjectIntHashMap[String]]()
    countSrc = new TObjectIntHashMap[String]()
    countTrg = new TObjectIntHashMap[String]()
    val sentence_library: RDD[String] = sc.sparkContext.textFile(sentenceFileName)
    sentence_library.foreach(line => {
      val lineSplit = line.split("\t")
      addSentence(lineSplit(0), lineSplit(1), lineSplit(2))
    })
  }

  def addSentence(srcLine: String, trgLine: String, alignLine: String) {
    val empty = "^\\s*$".r
    if (!empty.findAllMatchIn(srcLine).hasNext && !empty.findAllMatchIn(trgLine).hasNext && !empty.findAllMatchIn(alignLine).hasNext) {
      val srcContent = srcLine.split(" +")
      val trgContent = trgLine.split(" +")
      val at = new AlignmentTable(srcContent.length, trgContent.length)
      at.fillMatrix_aligntable(alignLine)
      val alignedCountSrc = new Array[Int](srcContent.length)
      val alignedCountTrg = new Array[Int](trgContent.length)
      for (i <- 0 until srcContent.length; j <- 0 until trgContent.length if at.isAligned(i, j)) {
        alignedCountSrc(i) += 1
        alignedCountTrg(j) += 1
        totalSrcWords += 1
        totalTrgWords += 1
        addPair(srcContent(i), trgContent(j))
      }
      for (i <- 0 until srcContent.length if alignedCountSrc(i) == 0) {
        addPair(srcContent(i), "<NULL>")
        totalSrcNull += 1
        totalSrcWords += 1
      }
      for (i <- 0 until trgContent.length if alignedCountTrg(i) == 0) {
        addPair("<NULL>", trgContent(i))
        totalTrgNull += 1
        totalTrgWords += 1
      }
    }
  }

  def addPair(src: String, trg: String) {
    addSource(src)
    addTarget(trg)
    var tmap: TObjectIntHashMap[String] = null
    if (!countPair.contains(src)) {
      tmap = new TObjectIntHashMap[String]()
      countPair += (src -> tmap)
    }
    else
      tmap = countPair(src)
    if (!tmap.containsKey(trg))
      tmap.put(trg, 1)
    else
      tmap.put(trg, tmap.get(trg) + 1)
  }

  def addSource(src: String) {
    if (!countSrc.containsKey(src))
      countSrc.put(src, 1)
    else
      countSrc.put(src, countSrc.get(src) + 1)
  }

  def addTarget(trg: String) {
    if (!countTrg.containsKey(trg))
      countTrg.put(trg, 1)
    else
      countTrg.put(trg, countTrg.get(trg) + 1)
  }

  def buildTables : Unit = {
    probC2E = new scala.collection.mutable.HashMap[String, TObjectFloatHashMap[String]]
    probE2C = new scala.collection.mutable.HashMap[String, TObjectFloatHashMap[String]]
    for (s <- countPair.keys) {
      val tempMap: TObjectIntHashMap[String] = countPair(s)
      val it = tempMap.iterator()
      while (it.hasNext) {
        it.advance()
        if (s.equals("<NULL>") || it.key().equals("<NULL>")) {
          var stable: TObjectFloatHashMap[String] = null
          if (probC2E.contains(s)) stable = probC2E(s)
          else {
            stable = new TObjectFloatHashMap[String]
            probC2E += (s -> stable)
          }
          stable.put(it.key(), (1.0 * it.value() / countSrc.get(s)).toFloat)

          if (probE2C.contains(it.key())) stable = probE2C(it.key())
          else {
            stable = new TObjectFloatHashMap[String]
            probE2C += (it.key() -> stable)
          }
          stable.put(s, (1.0 * it.value() / countTrg.get(it.key())).toFloat)
        }
      }
    }
  }
}
*/

object WordTranslationPro {
  def countWordPairs(sc: SparkSession, sentenceFileName: String, partitionNum: Int): RDD[(String, Int)] = {
    sc.sparkContext.textFile(sentenceFileName, partitionNum).flatMap(line => {
      val lineSplit = line.trim.split(Splitter.mainSplitter)
      try {
        lineSplit(2)
      } catch {
        case ex: ArrayIndexOutOfBoundsException => throw new ArrayIndexOutOfBoundsException(line)
      }
      emitWordPair(lineSplit(0), lineSplit(1), lineSplit(2))
    }).reduceByKey(_ + _)
  }

  def computeWordProbE2C(wordPairs: RDD[(String, Int)]): RDD[(String, Float)] = {
    //(wordE,(wordE|||wordC,count))
    val wordPairsE = wordPairs.map(entry => {
      val wordPair = entry._1.split(" \\|\\|\\| ")
      (wordPair(1), (wordPair(1) + SpecialString.mainSeparator + wordPair(0), entry._2))
    })
    //(wordE,totalCountE)
    val countWordE = wordPairs.map(entry => (entry._1.split(" \\|\\|\\| ")(1), entry._2)).reduceByKey(_ + _)
    //(wordE|||wordC,count/totalCountE)
    val wordTranslationE2C = wordPairsE.join(countWordE).map(entry => {
      (entry._2._1._1, entry._2._1._2.toFloat / entry._2._2)
    })
    wordTranslationE2C
  }

  def computeWordProbC2E(wordPairs: RDD[(String, Int)]): RDD[(String, Float)] = {
    //(wordC,(wordC|||wordE,count))
    val wordPairsC = wordPairs.map(entry => (entry._1.split(" \\|\\|\\| ")(0), entry))
    //(wordC,totalCountC)
    val countWordC = wordPairs.map(entry => (entry._1.split(" \\|\\|\\| ")(0), entry._2)).reduceByKey(_ + _)
    //(wordC|||wordE,count/totalCountC)
    val wordTranslationC2E = wordPairsC.join(countWordC).map(entry => {
      (entry._2._1._1, entry._2._1._2.toFloat / entry._2._2)
    })
    wordTranslationC2E
  }

  /*def groupWordProbC2E(sc: SparkSession, sentenceFileName: String, partitionNum: Int):RDD[(String,Float)] = {
    val sentence_library: RDD[String] = sc.sparkContext.textFile(sentenceFileName, partitionNum)
    val wordPairs = sentence_library.flatMap(line => {
      val lineSplit = line.split("\t")
      try {
        lineSplit(2)
      } catch {
        case ex: ArrayIndexOutOfBoundsException => throw new ArrayIndexOutOfBoundsException(line)
      }
      emitWordPair(lineSplit(0), lineSplit(1), lineSplit(2))
    }).map(entry => {
      val v = entry._1.split(Splitter.mainSplitter)
      (v(0), (v(1), entry._2))
    }).groupByKey().flatMap(entry => {

      val pairsMap: scala.collection.mutable.HashMap[String, Int] = new scala.collection.mutable.HashMap[String, Int]()
      var totalKey: Float = 0
      entry._2.foreach(p => {
        totalKey += p._2
        if (pairsMap.contains(p._1))
          pairsMap += (p._1 -> (pairsMap(p._1) + p._2))
        else
          pairsMap += (p._1 -> p._2)
      })

      val it: Iterator[(String, Int)] = pairsMap.iterator
      val resultArr = new Array[(String, Float)](pairsMap.size)
      val i = 0
      it.foreach(p => {
        val key = new StringBuffer()
        key.append(entry._1).append(SpecialString.mainSeparator).append(p._1)
        val value:Float = p._2/totalKey
        resultArr(i)= (key.toString,value)
      })
      resultArr
    })
    wordPairs
  }*/

  def emitWordPair(srcSentence: String, trgSentence: String, alignLine: String): Array[(String, Int)] = {
    val wordPairs = new mutable.ArrayBuffer[(String, Int)]
    val empty = "^\\s*$".r
    if (!empty.findAllMatchIn(srcSentence).hasNext && !empty.findAllMatchIn(trgSentence).hasNext && !empty.findAllMatchIn(alignLine).hasNext) {
      val srcContent = srcSentence.trim.split(" +")
      val trgContent = trgSentence.trim.split(" +")
      val at = new AlignmentTable(srcContent.length, trgContent.length)
      at.fillMatrix_aligntable(alignLine)
      val alignedCountSrc = new Array[Int](srcContent.length)
      val alignedCountTrg = new Array[Int](trgContent.length)
      for (i <- 0 until srcContent.length; j <- 0 until trgContent.length if at.isAligned(i, j)) {
        alignedCountSrc(i) += 1
        alignedCountTrg(j) += 1
        wordPairs += ((srcContent(i) + " ||| " + trgContent(j), 1))
      }
      for (i <- 0 until srcContent.length if alignedCountSrc(i) == 0) {
        wordPairs += ((srcContent(i) + " ||| " + "<NULL>", 1))
      }
      for (i <- 0 until trgContent.length if alignedCountTrg(i) == 0) {
        wordPairs += (("<NULL>" + " ||| " + trgContent(i), 1))
      }
    }
    wordPairs.toArray
  }

  def saveWordProInMap(wordPro: RDD[(String, Float)]): scala.collection.mutable.HashMap[String, Object2FloatOpenHashMap[String]] = {
    val prob : scala.collection.mutable.HashMap[String, Object2FloatOpenHashMap[String]] =
      new scala.collection.mutable.HashMap[String, Object2FloatOpenHashMap[String]]
    wordPro.collect().foreach(entry => {
      val wordPair = entry._1.split(" \\|\\|\\| ")
      var map: Object2FloatOpenHashMap[String] = null
      if (prob.contains(wordPair(0)))
        map = prob(wordPair(0))
      else {
        map = new Object2FloatOpenHashMap[String]
      }
      map.put(wordPair(1), entry._2)
      //map += (wordPair(1) -> entry._2)
      prob += (wordPair(0) -> map)
    })
    prob
   /* val prob = new HashMap[String, HashMap[String, Float]]
    wordPro.collect().foreach(entry => {
      val wordPair = entry._1.split(" \\|\\|\\| ")
      var tmap: HashMap[String, Float] = null
      if (prob.contains(wordPair(0)))
        tmap = prob(wordPair(0))
      else {
        tmap = new HashMap[String, Float]()
      }
      tmap += (wordPair(1) -> entry._2)
      prob += (wordPair(0) -> tmap)
    })
    prob*/
  }
}
