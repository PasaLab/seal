package edu.nju.pasalab.mt.LanguageModel.SLM

import breeze.util.BloomFilter
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import edu.nju.pasalab.mt.LanguageModel.Smoothing.GTSmoothing
import edu.nju.pasalab.mt.util.{CommonFileOperations, SntToCooc}
import edu.nju.pasalab.util.ExtractionParameters
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import edu.nju.pasalab.util.math.basic

import scala.collection._

/**
  * THis class is build a good turing n-gram language model with BloomFilter
  * Created by wuyan on 2016/3/20.
  */
class GTModel extends Serializable{
  var N = 0
  var GTS : Option[Array[GTSmoothing]] = None
  var bloomFilter :Option[Array[BloomFilter[Long]]] = None
  var prob :Option[Array[RDD[(Long, Double)]]] = None

  /**
    * construction function for GTModel
    *
    * @param sc is SparkContext
    * @param trainData is the trainSet
    * @param ep parameters
    */
  def this(sc: SparkSession, trainData : Array[RDD[(String, Int)]],  ep: ExtractionParameters){
    this()
    this.N = ep.N
    this.setAdjustedCount(sc, trainData, ep)
//  this.setBloomFilter(prob.get, ep.expectedErrorRate)
  }

  /**
    * this function is initialize adjustedCount
    *
    * @param sc is SparkContext
    * @param grams is the trainSet
    * @param ep parameters
    */
  def setAdjustedCount(sc: SparkSession, grams : Array[RDD[(String, Int)]], ep: ExtractionParameters): Unit ={

    /* val count = new Array[RDD[(Long, Double)]](N)
    val gts = new Array[GTSmoothing](N) */
    CommonFileOperations.deleteIfExists(ep.lmRootDir + "/gt")
    val dictFile = sc.sparkContext.textFile(SntToCooc.getLMDictPath(ep.lmRootDir)).filter(_.length > 0).map(line => {
      val sp = line.trim.split("\\s+")
      (sp(0), sp(1).toInt)
    })
    val dict : Int2ObjectOpenHashMap[String] = new Int2ObjectOpenHashMap[String]
    for (elem <- dictFile.collect()) {
      dict.put(elem._2, elem._1)
    }
    val idBroad = sc.sparkContext.broadcast(dict)

    for (i <- 0 until N) {
      //val indexing = index(i)
      val gram  = grams(i)
      if (i == 0) {
        val GTSBroadcast = sc.sparkContext.broadcast(new GTSmoothing(gram.map(x => (x._2, 1)).reduceByKey(_+_)))
        /*gts(i) = new GTSmoothing(grams.map(x => (x._2, 1)).reduceByKey(_+_))
        val GTSBroadcast = sc.sparkContext.broadcast(gts(i))*/

        val adjusted = gram.mapPartitions(part => {
          val GTS = GTSBroadcast.value
          val id = idBroad.value
          part.map(elem => {
            val split = elem._1.trim.split("\\s+")
            val sp = new StringBuilder(256)
            sp.append(id.get(split(0).toInt))
            for (i <- 1 until split.length)
              sp.append(" ").append(id.get(split(i)))
            sp.append("\t").append(basic.Divide(GTS.getSmoothedCount(elem._2), GTS.all_r.get * 1.0))
            sp.toString()
          })
        })

        adjusted.saveAsTextFile(SntToCooc.getGTGramPath(ep.lmRootDir, i+1))
        //count(i) = adjusted.join(indexing).map(x => (x._2._2, x._2._1))
      } else {
        //val gram = grams(i)
        val GTSBroadcast = sc.sparkContext.broadcast(new GTSmoothing(gram.map(x => (x._2, 1)).reduceByKey(_+_)))
        /* gts(i) = new GTSmoothing(grams.map(x => (x._2, 1)).reduceByKey(_+_))
        val GTSBroadcast = sc.sparkContext.broadcast(gts(i))
        val GTS_prefix = sc.sparkContext.broadcast(gts(i-1)) */

        val adjusted = gram.mapPartitions(part => {
          val GTS = GTSBroadcast.value
          val id = idBroad.value
          part.map(elem => {
            val split = elem._1.trim.split("\\s+")
            val sp = new StringBuilder(256)
            sp.append(id.get(split(0).toInt))
            for (i <- 1 until split.length)
              sp.append(" ").append(id.get(split(i).toInt))
            sp.append("\t").append(basic.Divide(GTS.getSmoothedCount(elem._2), GTS.all_r.get * 1.0))
            sp.toString()
          })
        })

        /* val groupBy = grams.mapPartitions(part => {
          part.map(elem => {
            val tmp = elem._1.substring(0, elem._1.lastIndexOf(" "))
            (tmp, (elem._1, elem._2))
          })
        }).groupByKey().map(elem => {
          val arr = elem._2.toArray
          val totalCount = arr.map(x => x._2).reduce(_+_)
          val res = arr.map(elem => (elem._1, elem._2, totalCount))
          res
        }).flatMap(x => x)

        val adjusted = groupBy.mapPartitions(part => {
          val GTS = GTSBroadcast.value
          val prefix_gts = GTS_prefix.value
          part.map(elem => {
            val sp = new StringBuilder(elem._1).append("\t")
            //(elem._1, Divide(GTS.getSmoothedCount(elem._2), prefix_gts.getSmoothedCount(elem._3)))
            val value = Math.log10(GTS.getSmoothedCount(elem._2)) - Math.log10(prefix_gts.getSmoothedCount(elem._3))
            + Math.log10(elem._2 + 1) -Math.log10(GTS.all_r.get * 1.0)
            sp.append(value)
            sp.toString()
          })
        })*/
        adjusted.saveAsTextFile(SntToCooc.getGTGramPath(ep.lmRootDir, i+1))
        //count(i) = adjusted.join(indexing).map(x => (x._2._2, x._2._1))
      }

    }
     /*GTS = Some(gts)
     prob = Some(count)*/
  }

  /**
    * this function wants get an IndexedRDD for adjustedCount
    *
    * @param adjusted is the adjustedCount for n-grams
    * @return an IndexedRDD
    */
  def getIndexedGrams(adjusted: Array[RDD[(Long, Double)]]) :Array[IndexedRDD[Long, Double]] = {
    val result = new Array[IndexedRDD[Long, Double]](N)
    for (i <- 0 until N) {
      result(i) = IndexedRDD(adjusted(i))
    }
    result
  }

  def getIndexedGrams(adjusted : RDD[(Long, Double)]) : IndexedRDD[Long, Double] ={
    IndexedRDD(adjusted)
  }


  /**
    * this function is initialize bloomFilter
    *
    * @param adjusted is the adjustedCount for n-grams
    * @param p is the value the expected error rate for BloomFilter
    */
  def setBloomFilter(adjusted: Array[RDD[(Long, Double)]], p: Double): Unit = {
    val filter : Array[BloomFilter[Long]] = new Array[BloomFilter[Long]](N)
    for (i <- 0 until N) {
      val set = adjusted(i).map(x => x._1)
      val expectNum = set.count() * 1.0
      val (m, k) = BloomFilter.optimalSize(expectNum, p)
      val bits = new java.util.BitSet
      val bloom = new BloomFilter[Long](m, k, bits)
      set.collect().map(x => bloom.+=(x))
      filter(i) = bloom
    }
    //bloomFilter = Some(filter)
  }
}
