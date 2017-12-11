package edu.nju.pasalab.mt.LanguageModel.spark.exec

import java.util.concurrent.TimeUnit
import edu.nju.pasalab.mt.LanguageModel.Perplexity.GSBEvaluation
import edu.nju.pasalab.mt.LanguageModel.SLM.StupidBackOffModel
import edu.nju.pasalab.util.ExtractionParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by wuyan on 2016/3/29.
  */
object GSBMain {
  /**
    * @param sc is SparkContext
    * @param trainData is trainData used for train a Google Stupid Backoff n-gram language model
    * @param ep
    */
  def run(sc : SparkSession, trainData : Array[RDD[(String, Int)]], count : Array[Long], ep: ExtractionParameters): Unit = {

    println("Build a SLM with Google Stupid Backoff .")
    val gsb0 = System.nanoTime()

    val gsbModel = new StupidBackOffModel(sc, trainData, count, ep)

    val tGSB = TimeUnit.MILLISECONDS.convert(System.nanoTime() - gsb0, TimeUnit.NANOSECONDS)
    println("Build n-gram Language Model with Google Stupid Backoff waste time " + tGSB + " ms .")

    /* println("\ncompute GSB perplexity use SparkSql and BloomFilter .")
    val gt2 = System.nanoTime()
    val SQLPerplexity = GSBEvaluation.computeGSBPerplexitySQL(gsbModel,testSet, indexing, N, sqlContext)
    val gt3 = System.nanoTime()
    val tGTSql = TimeUnit.MILLISECONDS.convert(gt3 - gt2, TimeUnit.NANOSECONDS)
    println("SQL perplexity\t" + SQLPerplexity + "\tQuery time " + tGTSql + " ms .")

    println("\ncompute GSB perplexity use SparkSql, but no BloomFilter .")
    val gt_2 = System.nanoTime()
    val SQLPerplexityNoBloom = GSBEvaluation.computeGSBPerplexitySQL_NOBloom(gsbModel, testSet, indexing, N, sqlContext)
    val gt_3 = System.nanoTime()
    val tGT_NO = TimeUnit.MILLISECONDS.convert(gt_3 - gt_2, TimeUnit.NANOSECONDS)
    println("SQL perplexity, But no bloom Filter\t" + SQLPerplexityNoBloom + "\tQuery time " + tGT_NO + " ms .")*/

    /* println("\ncompute GSB perplexity with IndexedRDD and BloomFilter .")
    val gt4 = System.nanoTime()
    val IndexedPerplexity = GSBEvaluation.computeGSBPerplexityIndexedRDD(gsbModel, testSet, indexing, N)
    val gt5 = System.nanoTime()
    val tGTIndexed = TimeUnit.MILLISECONDS.convert(gt5 - gt4, TimeUnit.NANOSECONDS)
    println("IndexedRDD perplexity\t" + IndexedPerplexity + "\tQuery time " + tGTIndexed + " ms .")

    println("\ncompute GSB perplexity with IndexedRDD, But, No BloomFilter .")
    val gt_4 = System.nanoTime()
    val IndexedPerplexity_NO = GSBEvaluation.computeGSBPerplexityIndexedRDD_NOBloom(gsbModel, testSet, indexing, N, offset)
    val gt_5 = System.nanoTime()
    val tGTIndexed_NO = TimeUnit.MILLISECONDS.convert(gt_5 - gt_4, TimeUnit.NANOSECONDS)
    println("Indexed RDD perplexity, but no Bloom Filter\t" + IndexedPerplexity_NO + "\tQuery time " + tGTIndexed_NO + " ms .")*/

    println("\n-------------------------------------------------------------Done-------------------------------------------------------------\n")
  }
}
