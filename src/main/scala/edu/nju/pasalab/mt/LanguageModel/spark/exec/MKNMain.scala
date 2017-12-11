package edu.nju.pasalab.mt.LanguageModel.spark.exec

import java.util.concurrent.TimeUnit

import edu.nju.pasalab.mt.LanguageModel.Perplexity.MKNEvaluation
import edu.nju.pasalab.mt.LanguageModel.SLM.MKNModel
import edu.nju.pasalab.util.ExtractionParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by wuyan on 2016/3/29.
  */
object MKNMain {
  /**
    * @param sc is SparkContext
    * @param trainData is trainData used for train a Kneser-Ney n-gram language model
    * @param count n-gram number
    * @param ep is the value the parameters
    */
  def run(sc : SparkSession, trainData : Array[RDD[(String, Int)]], count : Array[Long], ep: ExtractionParameters): Unit = {
    println("Build a SLM with Modified Kneser-Ney Smoothing .")
    val bd0 = System.nanoTime()

    val mknModel = new MKNModel(sc, trainData, count, ep)

    val buildTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - bd0, TimeUnit.NANOSECONDS)
    println("Build n-gram Language Model with Modified Kneser-Ney Smoothing waste time " + buildTime + " ms .")

   /* println("\ncompute MKN perplexity use SparkSql and BloomFilter .")
    val mknSQL1 = System.nanoTime()
    val SQLPerplexity = MKNEvaluation.computeMKNPerplexityWithSQL(mknModel, testSet, indexing, N, sqlContext, partNum)
    val mknSQL2 = System.nanoTime()
    val MKN_SQL = TimeUnit.MILLISECONDS.convert(mknSQL2 - mknSQL1, TimeUnit.NANOSECONDS)
    println("SQL perplexity:\t" + SQLPerplexity + "\tQuery time " + MKN_SQL + " ms .")

    println("\ncompute MKN perplexity use SparkSql, but no BloomFilter .")
    val mknSQL3 = System.nanoTime()
    val SQLPerplexityNoBloom = MKNEvaluation.computeMKNPerplexityWithSQLNoBloomFilter(mknModel, testSet, indexing, N, sqlContext, partNum)
    val mknSQL4 = System.nanoTime()
    val MKN_SQL_NO = TimeUnit.MILLISECONDS.convert(mknSQL4 - mknSQL3, TimeUnit.NANOSECONDS)
    println("SQL perplexity , But no bloom Filter\t" + SQLPerplexityNoBloom + "\tQuery time " + MKN_SQL_NO + " ms .")*/

   /*println("\ncompute MKN perplexity with IndexedRDD and BloomFilter .")
    val mknInd1 = System.nanoTime()
    val IndexedPerplexity = MKNEvaluation.computeMKNPerplexityWithIndexedRDD(mknModel, testSet, indexing, N, partNum)
    val mknInd2 = System.nanoTime()
    val MKN_Indexed = TimeUnit.MILLISECONDS.convert(mknInd2 - mknInd1, TimeUnit.NANOSECONDS)
    println("IndexedRDD perplexity\t" + IndexedPerplexity + "\tQuery time " + MKN_Indexed + " ms .")

    println("\ncompute MKN perplexity with IndexedRDD, But, No BloomFilter .")
    val mknInd3 = System.nanoTime()
    val indexedPerplexity_NO = MKNEvaluation.computeMKNPerplexityWithIndexedRDDNoBloomFilter(mknModel, testSet, indexing, N, partNum, offset)
    val mknInd4 = System.nanoTime()
    val MKN_Indexed_NO = TimeUnit.MILLISECONDS.convert(mknInd4 - mknInd3, TimeUnit.NANOSECONDS)
    println("Indexed RDD perplexity, but no Bloom Filter\t" + indexedPerplexity_NO + "\tQuery time " + MKN_Indexed_NO + " ms .")*/

   /*
    // deprecated
    println("\ncompute KN perplexity with Original method .")
    val MKN1 = System.nanoTime()
    val s2= Evaluation.computeMKNPerplexity(trainData, testSet.collect(), N, sqlContext)
    val MKN2 = System.nanoTime()
    val TMKN = TimeUnit.SECONDS.convert(MKN2 - MKN1, TimeUnit.NANOSECONDS)
    println("original perplexity\t" + s2 + "\tQuery time " + TMKN + " s .")*/
    
    println("\n-------------------------------------------------------------Done-------------------------------------------------------------\n")
  }
}
