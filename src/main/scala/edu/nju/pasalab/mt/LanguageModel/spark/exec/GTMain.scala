package edu.nju.pasalab.mt.LanguageModel.spark.exec

import java.util.concurrent.TimeUnit
import edu.nju.pasalab.mt.LanguageModel.Perplexity.GTEvaluation
import edu.nju.pasalab.mt.LanguageModel.SLM.GTModel
import edu.nju.pasalab.util.ExtractionParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SQLContext}

/**
  * Created by YWJ on 2016/3/28.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object GTMain {
  /**
    * @param sc is SparkContext
    * @param trainData is trainData used for train a Kneser-Ney n-gram language model
    * @param ep is the value the N
    */
  def run(sc : SparkSession, trainData : Array[RDD[(String, Int)]], ep: ExtractionParameters): Unit = {

    println("\nBuild a SLM with good turing .")
    val gt0 = System.nanoTime()

    val gtModel = new GTModel(sc, trainData, ep)

    val tGT = TimeUnit.MILLISECONDS.convert(System.nanoTime() - gt0, TimeUnit.NANOSECONDS)
    println("Build n-gram Language Model with good turing waste time " + tGT + " ms .")


    /*println("\ncompute GT perplexity use SparkSql and BloomFilter .")
    val gt2 = System.nanoTime()
    val SQLPerplexity = GTEvaluation.computeGTPerplexityWithSQL(gtModel, testSet, indexing, N, sqlContext)
    val gt3 = System.nanoTime()
    val tGTSql = TimeUnit.MILLISECONDS.convert(gt3 - gt2, TimeUnit.NANOSECONDS)
    println("SQL perplexity\t" + SQLPerplexity + "\tQuery time " + tGTSql + " ms .")

    println("\ncompute GT perplexity use SparkSql, but no BloomFilter .")
    val gt_2 = System.nanoTime()
    val SQLPerplexityNoBloom = GTEvaluation.computeGTPerplexityWithSQLNoBloomFilter(gtModel, testSet, indexing, N, sqlContext)
    val gt_3 = System.nanoTime()
    val tGT_NO = TimeUnit.MILLISECONDS.convert(gt_3 - gt_2, TimeUnit.NANOSECONDS)
    println("SQL perplexity, But no bloom Filter\t" + SQLPerplexityNoBloom + "\tQuery time " + tGT_NO + " ms .")*/

    /*println("\ncompute GT perplexity with IndexedRDD and BloomFilter .")
    val gt4 = System.nanoTime()
    val IndexedPerplexity = GTEvaluation.computeGTPerplexityWithIndexedRDD(gtModel, testSet, indexing,ep.N)
    val gt5 = System.nanoTime()
    val tGTIndexed = TimeUnit.MILLISECONDS.convert(gt5 - gt4, TimeUnit.NANOSECONDS)
    println("IndexedRDD perplexity\t" + IndexedPerplexity + "\tQuery time " + tGTIndexed + " ms .")

    println("\ncompute GT perplexity with IndexedRDD, But, No BloomFilter .")
    val gt_4 = System.nanoTime()
    val IndexedPerplexity_NO = GTEvaluation.computeGTPerplexityWithIndexedRDDNoBloomFilter(gtModel, testSet, indexing, ep.N, ep.offset)
    val gt_5 = System.nanoTime()
    val tGTIndexed_NO = TimeUnit.MILLISECONDS.convert(gt_5 - gt_4, TimeUnit.NANOSECONDS)
    println("Indexed RDD perplexity, but no Bloom Filter\t" + IndexedPerplexity_NO + "\tQuery time " + tGTIndexed_NO + " ms .")*/

    /* deprecated
    println("\ncompute GT perplexity with Original method .")
    val gtN1 = System.nanoTime()
    val s1 = Evaluation.computePerplexity(trainData, testSet.collect(), N, sqlContext)
    val gtN2 = System.nanoTime()
    val TGT = TimeUnit.SECONDS.convert(gtN2 - gtN1, TimeUnit.NANOSECONDS)
    println("original perplexity\t" + s1 + "\tQuery time " + TGT + " s .")*/

    println("\n-------------------------------------------------------------Done-------------------------------------------------------------\n")
  }
}
