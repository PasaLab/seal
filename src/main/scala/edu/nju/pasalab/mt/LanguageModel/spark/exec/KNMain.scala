package edu.nju.pasalab.mt.LanguageModel.spark.exec

import java.util.concurrent.TimeUnit

import edu.nju.pasalab.mt.LanguageModel.Perplexity.KNEvaluation
import edu.nju.pasalab.mt.LanguageModel.SLM.KNModel
import edu.nju.pasalab.util.ExtractionParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by wuyan on 2016/3/29.
  */
object KNMain {
  /**
    * @param sc is SparkContext
    * @param trainData is trainData used for train a Kneser-Ney n-gram language model
    * @param ep parameters
    */
  def run(sc : SparkSession, trainData : Array[RDD[(String, Int)]], count : Array[Long], ep: ExtractionParameters): Unit = {

    println("Build a SLM with Kneser-Ney Smoothing .")
    val bd0 = System.nanoTime()

    val knModel = new KNModel(sc, trainData, count, ep)

    val buildTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - bd0, TimeUnit.NANOSECONDS)
    println("Build n-gram Language Model with Kneser-Ney Smoothing waste time " + buildTime + " ms .")

    /*  println("\ncompute KN perplexity use SparkSql and BloomFilter .")
    val knSQL1 = System.nanoTime()
    val SQLPerplexity = KNEvaluation.computeKNPerplexityWithSQL(knModel, testSet, indexing, N, sqlContext, partNum)
    val knSQL2 = System.nanoTime()
    val KN_SQL = TimeUnit.MILLISECONDS.convert(knSQL2 - knSQL1, TimeUnit.NANOSECONDS)
    println("SQL perplexity:\t" + SQLPerplexity + "\tQuery time " +KN_SQL + " ms .")

    println("\ncompute KN perplexity use SparkSql, but no BloomFilter .")
    val knSQL3 = System.nanoTime()
    val SQLPerplexityNoBloom = KNEvaluation.computeKNPerplexityWithSQLNoBloomFilter(knModel, testSet, indexing, N, sqlContext, partNum)
    val knSQL4 = System.nanoTime()
    val KN_SQL_NO = TimeUnit.MILLISECONDS.convert(knSQL4 - knSQL3, TimeUnit.NANOSECONDS)
    println("SQL perplexity , But no bloom Filter\t" + SQLPerplexityNoBloom + "\tQuery time " +KN_SQL_NO + " ms .")*/

   /*println("\ncompute KN perplexity with IndexedRDD and BloomFilter .")
    val knInd1 = System.nanoTime()
    val IndexedPerplexity = KNEvaluation.computeKNPerplexityWithIndexedRDD(knModel, testSet, indexing, N, partNum)
    val knInd2 = System.nanoTime()
    val KN_Indexed = TimeUnit.MILLISECONDS.convert(knInd2 - knInd1, TimeUnit.NANOSECONDS)
    println("IndexedRDD perplexity\t" + IndexedPerplexity + "\tQuery time " + KN_Indexed + " ms .")

    println("\ncompute KN perplexity with IndexedRDD, But, No BloomFilter .")
    val knInd3 = System.nanoTime()
    val indexedPerplexity_NO = KNEvaluation.computeKNPerplexityWithIndexedRDDNoBloomFilter(knModel, testSet, indexing, N, partNum, offset)
    val knInd4 = System.nanoTime()
    val KN_Indexed_NO = TimeUnit.MILLISECONDS.convert(knInd4 - knInd3, TimeUnit.NANOSECONDS)
    println("Indexed RDD perplexity, but no Bloom Filter\t" + indexedPerplexity_NO + "\tQuery time " + KN_Indexed_NO + " ms .")*/
    // deprecated
   /*println("\ncompute KN perplexity with Original method .")
    val KN1 = System.nanoTime()
    val s2= Evaluation.computeKNPerplexity(trainData, testSet.collect(), N, sqlContext)
    val KN2 = System.nanoTime()
    val TKN = TimeUnit.SECONDS.convert(KN2 - KN1, TimeUnit.NANOSECONDS)
    println("original perplexity\t" + s2 + "\tQuery time " + TKN + " s .")*/

    println("\n-------------------------------------------------------------Done-------------------------------------------------------------\n")
  }
}
