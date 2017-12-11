package edu.nju.pasalab.mt.LanguageModel.Perplexity

import edu.nju.pasalab.mt.LanguageModel.SLM.GTModel
import edu.nju.pasalab.mt.LanguageModel.util.{Schema, toolFun}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wuyan on 2016/3/19.
  */
object GTEvaluation {
  /**
    * this function use SparkSQL to query
    *
    * @param gtModel is a Good Turing n-gram Language Model
    * @param testSet is the test Set for evaluation Good Turing n-gram Language Model
    * @param index an unify indexing for word sequence of n-gram
    * @param n is the value the N
    * @param sc sparkContext
    * @return the Perplexity of Good Turing n-gram Language Model
    */

  def computeGTPerplexityWithSQL(gtModel : GTModel , testSet : RDD[String], index: Array[RDD[(String, Long)]],
                               n: Int, sc : SparkSession) : Double = {

    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 0 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))
      prob += computeGTProbWithSQL(gtModel, grams, index(i), sc, i)
    }
    math.exp((0.0 - prob)/num)
  }
  def computeGTPerplexityWithSQLNoBloomFilter(gtModel : GTModel , testSet : RDD[String], index: Array[RDD[(String, Long)]],
                                            n: Int, sc : SparkSession) : Double = {
    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 0 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))

      prob += computeGTProbWithSQLWithoutBloomFilter(gtModel, grams, index(i), sc, i)
    }
    math.exp((0.0 - prob)/num)
  }

  /**
    * this function use IndexedRDD to query
    *
    * @param gtModel  is a Good Turing n-gram Language Model
    * @param testSet is the test Set for evaluation Good Turing n-gram Language Model
    * @param index an unify indexing for word sequence of n-gram
    * @param n is the value the N
    * @return the Perplexity of Good Turing n-gram Language Model
    */

  def computeGTPerplexityWithIndexedRDD(gtModel : GTModel , testSet : RDD[String], index: Array[RDD[(String, Long)]],
                                      n: Int) : Double = {

    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 0 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))
      prob += computeGTProbWithIndexedRDD(gtModel, grams, index(i), i)
    }
    math.exp((0.0 - prob)/num)
  }

  def computeGTPerplexityWithIndexedRDDNoBloomFilter(gtModel : GTModel , testSet : RDD[String], index: Array[RDD[(String, Long)]],
                                                   n: Int, offset: Int) : Double = {

    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 0 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))
      prob += computeGTProbWithIndexedRDDWithoutBloomFilter(gtModel, grams, index(i), i, offset)
    }
    math.exp((0.0 - prob)/num)
  }

  def computeGTProbWithSQL(gtModel : GTModel, gram : RDD[String], index : RDD[(String, Long)],
                           sc:SparkSession, pos :Int): Double = {
    var probArray = 0.0
    /**
      * id -> (ID, count)
      */
    val id = gram.map(x => (x, 1L)).reduceByKey(_+_).join(index).map(x => (x._2._2, x._2._1)).collect()
    println((pos+1) + "-gram length " + gram.count() + " id length: " + id.length)

    val dataFrame = sc.sqlContext.createDataFrame(gtModel.prob.get(pos).map(p => Row(p._1, p._2)), Schema.GT_Schema).cache()
    val bloomFilter = gtModel.bloomFilter.get(pos)
    //OOV represents out of vocabulary
    val OOV = Divide(gtModel.GTS.get(pos).N1.get, gtModel.GTS.get(pos).all_r.get)

    id.map(elem => {
      if (bloomFilter.contains(elem._1)) {
        val count = dataFrame.filter(dataFrame("ID") === elem._1).collect()
        if (count.length > 0) {
          probArray += math.log(count(0).getDouble(1) * elem._2)
        } else {
          probArray += math.log(OOV * elem._2)
        }
      } else {
        probArray += math.log(OOV * elem._2)
      }
    })
    dataFrame.unpersist()
    probArray
  }

  def computeGTProbWithSQLWithoutBloomFilter(gtModel : GTModel, gram : RDD[String], index : RDD[(String, Long)],
                                             sc:SparkSession, pos :Int) : Double = {
    var probArray = 0.0
    /**
      * id -> (ID, count)
      */
    val id = gram.map(x => (x, 1L)).reduceByKey(_+_).join(index).map(x => (x._2._2, x._2._1)).collect()

    val dataFrame = sc.sqlContext.createDataFrame(gtModel.prob.get(pos).map(p => Row(p._1, p._2)), Schema.GT_Schema).cache()
    //OOV represents out of vocabulary
    val OOV = Divide(gtModel.GTS.get(pos).N1.get, gtModel.GTS.get(pos).all_r.get)

    id.map(elem => {
      val count = dataFrame.filter(dataFrame("ID") === elem._1).collect()
      if (count.length > 0) {
        probArray += math.log(count(0).getDouble(1) * elem._2)
      } else {
        probArray += math.log(OOV * elem._2)
      }
    })

    dataFrame.unpersist()
    probArray
  }

  def computeGTProbWithIndexedRDD(gtModel : GTModel, gram : RDD[String], index : RDD[(String, Long)], pos : Int) : Double = {
    var probArray = 0.0

    /**
      * id -> (ID, count)
      */
    val id = gram.map(x => (x, 1L)).reduceByKey(_+_).join(index).map(x => (x._2._2, x._2._1)).collect()
    println(pos + "-gram length " + gram.count() + " id length: " + id.length)

    val indexed = gtModel.getIndexedGrams(gtModel.prob.get(pos))
    val bloomFilter = gtModel.bloomFilter.get(pos)
    //OOV represents out of vocabulary
    val OOV = Divide(gtModel.GTS.get(pos).N1.get, gtModel.GTS.get(pos).all_r.get)

    id.map(x => {
      if (bloomFilter.contains(x._1)) {
        val count = indexed.get(x._1)
        if (count.isEmpty) {
          probArray += math.log(OOV * x._2)
        } else {
          probArray += math.log(count.get * x._2)
        }
      } else {
        probArray += math.log(OOV * x._2)
      }
    })
    probArray
  }

  def computeGTProbWithIndexedRDDWithoutBloomFilter(gtModel : GTModel, gram : RDD[String], index : RDD[(String, Long)], pos : Int, offset :Int) : Double = {
    var probArray = 0.0

    val id = gram.map(x => (x, 1)).reduceByKey(_+_)
      .join(index).map(x => (x._2._2, x._2._1)).collect()

    val indexed = gtModel.getIndexedGrams(gtModel.prob.get(pos)).cache()
    //OOV represents out of vocabulary
    val OOV = Divide(gtModel.GTS.get(pos).N1.get, gtModel.GTS.get(pos).all_r.get)
    var i = 0
    //val offset = 1000

    while (i < id.length) {
      val buffer = new ArrayBuffer[(Long, Int)]()
      for (index <- i until (i + offset)  if index < id.length) {
        buffer.append(id(index))
      }

      val map = indexed.multiget(buffer.toArray.map(x => x._1))

      for (elem <- buffer.toArray) {
        if (map.contains(elem._1)) {
          probArray += math.log(map(elem._1) * elem._2)
        } else {
          probArray += math.log(OOV * elem._2)
        }
      }

      i += offset
    }

    indexed.unpersist()
    probArray
  }

  def Divide(a : Long, b : Long) : Double = {
    (a * 1.0) / (b * 1.0)
  }
}
