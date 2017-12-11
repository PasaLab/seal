package edu.nju.pasalab.util

import edu.nju.pasalab.mt.extraction.dataStructure.Splitter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by BranY on 2016.11.18.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object PreProcess{

  val MAX_WORD:Long = 0x40000000L
  def buildVocab(corpus:RDD[String],sc:SparkContext): scala.collection.mutable.HashMap[String,Int] ={
    var vocabMap = new scala.collection.mutable.HashMap[String,Int]
    val distinctWords = corpus.flatMap(_.split("\\s")).distinct()
    var counter = 1
    distinctWords.collect().foreach(s=>{
      vocabMap+=(s->counter)
      counter += 1
    })
   vocabMap
  }


  def transformCorpus(fullCorpus:RDD[String],sourceVocab:scala.collection.mutable.HashMap[String,Int],
                      targetVocab:scala.collection.mutable.HashMap[String,Int],sc:SparkContext): RDD[String] = {


    val sourceVocabBroad = sc.broadcast(sourceVocab)
    val targetVocabBroad = sc.broadcast(targetVocab)

    val transformedCorpus = fullCorpus.filter(_.trim.split("\t")(0).split("\\s").length<=100).map(s => {
      val lineSplit = s.trim.split("\t")
      val sourceSplit = lineSplit(0).split("\\s")
      val targetSplit = lineSplit(1).split("\\s")
      val srcVocab: scala.collection.mutable.HashMap[String,Int] = sourceVocabBroad.value
      val trgVocab: scala.collection.mutable.HashMap[String,Int] = targetVocabBroad.value
     // println("broadcast:" + trgVocab("reported"))
      val text: StringBuffer = new StringBuffer()
      sourceSplit.foreach(s => text.append(srcVocab(s)).append(" "))
      text.append("|||")
      targetSplit.foreach(s => text.append(" ").append(trgVocab(s)))
      text.toString
    })
    transformedCorpus
  }

  def initialization_hmm(corpusPath:String,transformCorpusPath:String,numPartition:Int,iterationPath:String,transitionPath:String,initState:String, sc:SparkContext){
        val fullCorpus = sc.textFile(corpusPath, numPartition).filter(!_.startsWith("= = = = = = = =")).cache()
        val sourceVocab: scala.collection.mutable.HashMap[String,Int] = PreProcess.buildVocab(fullCorpus.map(_.split("\t")(0)), sc)
        val targetVocab: scala.collection.mutable.HashMap[String,Int] = PreProcess.buildVocab(fullCorpus.map(_.split("\t")(1)), sc)
        
        val transformedCorpus = PreProcess.transformCorpus(fullCorpus, sourceVocab, targetVocab, sc)
        
        val longestSourceSentence= transformedCorpus.map(_.split(Splitter.mainSplitter)(0).split("\\s").length).top(1)
        fullCorpus.unpersist()
        transformedCorpus.cache()
        transformedCorpus.saveAsTextFile(transformCorpusPath)

        val longestLength = longestSourceSentence(0)
        val init_state_prob = Math.log(1.0/longestLength)
        PreProcess.initTranslationTable_hdfs(transformedCorpus, Math.log(1.0 / targetVocab.size),iterationPath)
        println("init_state_prob: "+ init_state_prob)
        println("longest sentence: "+ longestSourceSentence(0))
        sc.parallelize(0 to longestLength-1).flatMap(t=>{
          val arr = Array.ofDim[(Int,Double)](longestLength)
          for(i<-0 to longestLength-1)
            arr.update(i, (t*longestLength+i,init_state_prob))
          arr
        }).map(t=>t._1+"\t"+t._2).saveAsTextFile(transitionPath)

        sc.parallelize(0 to longestSourceSentence(0)-1).map(_+"\t"+init_state_prob).saveAsTextFile(initState)
  }

  def initialization_ibm_one(corpusPath:String,transformCorpusPath:String,numPartition:Int,iterationPath:String,sc:SparkContext): Unit ={
    val fullCorpus = sc.textFile(corpusPath,numPartition).cache()
    val sourceVocab: scala.collection.mutable.HashMap[String,Int] = PreProcess.buildVocab(fullCorpus.map(_.split("\t")(0)), sc)
    val targetVocab: scala.collection.mutable.HashMap[String,Int] = PreProcess.buildVocab(fullCorpus.map(_.split("\t")(1)), sc)

    val transformedCorpus = PreProcess.transformCorpus(fullCorpus, sourceVocab, targetVocab, sc)

    fullCorpus.unpersist()
    transformedCorpus.cache()
    transformedCorpus.saveAsTextFile(transformCorpusPath)

    PreProcess.initTranslationTable_hdfs(transformedCorpus, 1.0f / targetVocab.size,iterationPath)
  }

  def initialization_ibm_two(corpusPath:String,transformCorpusPath:String,numPartition:Int,iterationPath:String,alignmentPath:String,sc:SparkContext){
    val fullCorpus = sc.textFile(corpusPath,numPartition).cache()
    val sourceVocab: scala.collection.mutable.HashMap[String,Int] = PreProcess.buildVocab(fullCorpus.map(_.split("\t")(0)), sc)
    val targetVocab: scala.collection.mutable.HashMap[String,Int] = PreProcess.buildVocab(fullCorpus.map(_.split("\t")(1)), sc)

    val transformedCorpus = PreProcess.transformCorpus(fullCorpus, sourceVocab, targetVocab, sc)

    fullCorpus.unpersist()
    transformedCorpus.cache()
    transformedCorpus.saveAsTextFile(transformCorpusPath)

    PreProcess.initTranslationTable_hdfs(transformedCorpus, 1.0f / targetVocab.size,iterationPath)

    transformedCorpus.flatMap(t=>{
      val lineSplit = t.split(Splitter.mainSplitter)
      val source = lineSplit(0).split("\\s")
      val target = lineSplit(1).split("\\s")
      val l_e = target.length
      val l_f = source.length
      val aligner= Array.ofDim[String](l_e*(l_f+1))
      val uniform_v = 1.0/(l_f+1)
      var index = 0
      for(j<-1 to l_e)
        for(i<-0 to l_f) {
          aligner(index) = i + "|" + j + "|" + l_e + "|" + l_f + "\t" + uniform_v
          index += 1
        }
      aligner
    }).saveAsTextFile(alignmentPath)

  }

  def initTranslationTable_onedimension(corpus:RDD[String],divide:Double, iterationPath:String) {
    val translationTable = corpus.flatMap(s=>{
      val lineSplit = s.trim.split(Splitter.mainSplitter)
      val sourceSplit = lineSplit(0).split("\\s")
      val targetSplit = lineSplit(1).split("\\s")
      val arrayKV:Array[Long]= new Array(sourceSplit.length*targetSplit.length)
      var index = 0
      for(i <-0 until targetSplit.length){
        for(j<-0 until sourceSplit.length) {
          arrayKV(index) = targetSplit(i).toInt*MAX_WORD + sourceSplit(j).toInt
          index += 1
        }
      }
      arrayKV
    }).distinct()
    translationTable.map(_+"\t"+divide).saveAsTextFile(iterationPath)
  }

  def initTranslationTable_hdfs(corpus:RDD[String],divide:Double, initPath:String) {
    val translationTable = corpus.flatMap(s=>{
      val lineSplit = s.trim.split(Splitter.mainSplitter)
      val sourceSplit = lineSplit(0).split("\\s")
      val targetSplit = lineSplit(1).split("\\s")
      val arrayKV:Array[String]= new Array(sourceSplit.length*targetSplit.length)
      var index = 0
      for(i <-0 until targetSplit.length){
        for(j<-0 until sourceSplit.length) {
          arrayKV(index) = targetSplit(i) + "|" + sourceSplit(j)
          index += 1
        }
      }
      arrayKV
    }).distinct()
    translationTable.map(_+"\t"+divide).saveAsTextFile(initPath)
  }

  def initTranslationTable_broadcast(corpus:RDD[String],divide:Double):scala.collection.mutable.HashMap[String,Double] = {
    var transMap:scala.collection.mutable.HashMap[String,Double] = new scala.collection.mutable.HashMap[String,Double]
    val translationTable = corpus.flatMap(s=>{
      val lineSplit = s.trim.split(Splitter.mainSplitter)
      val sourceSplit = lineSplit(0).split("\\s")
      val targetSplit = lineSplit(1).split("\\s")
      val arrayKV:Array[String]= new Array(sourceSplit.length*targetSplit.length)
      var index = 0
      for(i <-0 until targetSplit.length){
        for(j<-0 until sourceSplit.length) {
          arrayKV(index) = targetSplit(i) + "|" + sourceSplit(j)
          index += 1
        }
      }
      arrayKV
    }).distinct()
    //translationTable.map(_+"\t"+divide).saveAsTextFile(inputPath)
    translationTable.collect().foreach(s=> transMap+=(s->divide))
    transMap
  }
}
