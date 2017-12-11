package edu.nju.pasalab.mt.LanguageModel.SLM

import breeze.util.BloomFilter
import edu.nju.pasalab.mt.LanguageModel.util.NGram
import edu.nju.pasalab.mt.util.{CommonFileOperations, SntToCooc}
import edu.nju.pasalab.util.ExtractionParameters
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD


import edu.nju.pasalab.util.gramPartitioner
import edu.nju.pasalab.util.math._

/**
  * This class used Google Stupid BackOff Smoothing to build a n-gram Language Model
  * * for special :
  * 1-gram we add three special token : <s> </s> <unk>, we mark:
  * 0 <s> backoff
  * p </s> 0
  * p <unk> 0
  * Created by wuyan on 2016/3/22.
  */
class StupidBackOffModel extends Serializable{
  var N = 0
  val alpha = 0.4
  var prob : Option[Array[RDD[(Long, Double)]]] = None
  var bloomFilter :Option[Array[BloomFilter[Long]]] = None

  /**
    * construction function for Kneser-Ney Model
    *
    * @param sc is SparkContext
    * @param trainData is the trainSet
    * @param ep parameters
    */
  //index: Array[RDD[(String, Long)]]
  def this(sc : SparkSession, trainData : Array[RDD[(String, Int)]],
           count : Array[Long], ep: ExtractionParameters) {
    this()
    this.N = ep.N
    this.set(sc, trainData, count, ep)
    //this.setBloomFilter(this.prob.get, ep.expectedErrorRate)
  }


  /**
    * @param sc is SparkContext
    * @param grams is the trainSet
    * @param ep parameters
    */
  def set(sc : SparkSession, grams : Array[RDD[(String, Int)]], count : Array[Long],ep: ExtractionParameters): Unit ={
   // val PROB = new Array[RDD[(Long, Double)]](N)
    CommonFileOperations.deleteIfExists(ep.lmRootDir + "/gsb")
    val dictFile = sc.sparkContext.textFile(SntToCooc.getLMDictPath(ep.lmRootDir)).filter(_.length > 0).map(line => {
      val sp = line.trim.split("\\s+")
      (sp(0), sp(1).toInt)
    })

    val dict : Int2ObjectOpenHashMap[String] = new Int2ObjectOpenHashMap[String]
    for (elem <- dictFile.collect()) {
      dict.put(elem._2, elem._1)
    }
    val idBroad = sc.sparkContext.broadcast(dict)
    //println(N)
    for (i <- 0 until N) {
     // val indexing = index(i)
      if (i == 0) {
        val gram = grams(i).repartitionAndSortWithinPartitions(new HashPartitioner(ep.partitionNum))
        //.partitionBy(new HashPartitioner(ep.partitionNum))
        //val count = gram.count()
        val gsb = gram.mapPartitions(part => {
          val id = idBroad.value
          part.map(elem => {
            val split = elem._1.trim.split("\\s+")
            val sb = new StringBuilder(128)
            val tmp = split(0).toInt

            if (tmp == 0) {
              sb.append(0).append("\t").append("<s>").append("\t").append(Math.log10(alpha))
            } else {
              sb.append(basic.Divide(elem._2, count(i))).append("\t").append(id.get(tmp.toInt))
              if (tmp == 1) {
                sb.append("\t").append(0)
              } else{
                sb.append("\t").append(Math.log10(alpha))
              }
            }
            sb.toString()
          })
        })
        gsb.union(sc.sparkContext.makeRDD(Array(basic.Divide(1.0, count(i)) + "\t" + "unk\t 0.0")))
          .repartition(ep.partitionNum)
          .saveAsTextFile(SntToCooc.getGSBGramPath(ep.lmRootDir, i+1))

        /*PROB(i) = grams.map(x => (x._1, Divide(x._2, count)))
          .join(indexing)
          .map(elem => (elem._2._2, elem._2._1))*/
      } else {

        val gram = simpleMap(grams(i).repartitionAndSortWithinPartitions(new gramPartitioner(ep.partitionNum)))
          //.partitionBy(new gramPartitioner(ep.partitionNum)))

        val gsb = gram.join(gram.map(elem => (elem._1, elem._2._2)).reduceByKey(_+_)).mapPartitions(part => {
          val id = idBroad.value
          part.map(elem =>{
            val sp = new StringBuilder(128)
            val split = elem._2._1._1.trim.split("\\s+")
            sp.append(basic.Divide(elem._2._1._2, elem._2._2)).append("\t").append(id.get(split(0).toInt))

            for (i <- 1 until split.length)
              sp.append(" ").append(id.get(split(i).toInt))

            if (i != 4) {
              sp.append("\t").append(Math.log10(alpha))
            }
            sp.toString()
          })
        })
        gsb.saveAsTextFile(SntToCooc.getGSBGramPath(ep.lmRootDir, i+1))
       /* PROB(i) = prop
          .join(prop.map(elem => (elem._1, elem._2._2)).reduceByKey(_+_))
          .map(x => (x._2._1._1, Divide(x._2._1._2, x._2._2)))
          .join(indexing)
          .map(elem => (elem._2._2, elem._2._1))*/
      }
    }
    //prob = Some(PROB)
  }

  def simpleMap(data : RDD[(String, Int)]) : RDD[(String, (String, Int))] ={
    val prop = data.map(elem => {
      val tmp = elem._1.substring(0, elem._1.lastIndexOf(" "))
      (tmp, (elem._1, elem._2))
    })
    prop
  }

  def setBloomFilter(arr : Array[RDD[(Long, Double)]], p : Double): Unit = {
    val filter : Array[BloomFilter[Long]] = new Array[BloomFilter[Long]](N)
    for (i <- 0 until N) {
      val set = arr(i).map(x => x._1)
      val (m, k) = BloomFilter.optimalSize(set.count(), p)
      val bloom = new BloomFilter[Long](m ,k, new java.util.BitSet)
      set.collect().map(x => bloom.+=(x))
      filter(i) = bloom
    }
    //bloomFilter = Some(filter)
  }
}
