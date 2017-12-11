package edu.nju.pasalab.mt.LanguageModel.Perplexity

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import edu.nju.pasalab.mt.LanguageModel.SLM.MKNModel
import edu.nju.pasalab.util.math.BIGDECIMAL

//import edu.nju.pasalab.mt.LanguageModel.util.{Schema, SpecialString, Splitter, toolFun}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import edu.nju.pasalab.mt.LanguageModel.util._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by wuyan on 2016/3/30.
  */
object MKNEvaluation {
  /**
    * this function compute Perplexity with SparkSQL
 *
    * @param mknModel is a Modified Kneser-Ney n-gram Language Model
    * @param testSet is the test Set for evaluation Good Turing n-gram Language Model
    * @param index an unify indexing for word sequence of n-gram
    * @param n is the value the N
    * @return the Perplexity of Good Turing n-gram Language Model
    */
  def computeMKNPerplexityWithSQL(mknModel: MKNModel, testSet : RDD[String], index: Array[RDD[(String, Long)]],
                               n: Int, sc : SparkSession, partNum : Int) : Double = {
    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val array = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 1 until n) {
      val grams = array.filter(x => x.split(" ").length == (i+1))
      prob += computeMKNProbWithSQL(mknModel, grams, index, sc, i, partNum)
    }
    math.exp((0.0 -prob)/num)
  }

  def computeMKNPerplexityWithSQLNoBloomFilter(mknModel: MKNModel, testSet : RDD[String], index: Array[RDD[(String, Long)]],
                               n: Int, sc : SparkSession, partNum : Int) : Double = {
    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val array = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 1 until n) {
      val grams = array.filter(x => x.split(" ").length == (i+1))
      prob += computeMKNProbWithSQLNoBloomFilter(mknModel, grams, index, sc, i, partNum)
    }
    math.exp((0.0 -prob)/num)
  }

  /**
    * this function use IndexedRDD to query
    *
    * @param mknModel  is a Modified Kneser-Ney n-gram Language Model
    * @param testSet is the test Set for evaluation Good Turing n-gram Language Model
    * @param index an unify indexing for word sequence of n-gram
    * @param n is the value the N
    * @return the Perplexity of Good Turing n-gram Language Model
    */

  def computeMKNPerplexityWithIndexedRDD(mknModel: MKNModel, testSet : RDD[String], index: Array[RDD[(String, Long)]],
                                      n: Int, partNum : Int) : Double = {

    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 1 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))
      prob += computeMKNProbWithIndexedRDD(mknModel, grams, index, i, partNum)
    }
    math.exp((0.0 - prob)/num)
  }

  def computeMKNPerplexityWithIndexedRDDNoBloomFilter(mknModel: MKNModel, testSet : RDD[String], index: Array[RDD[(String, Long)]],
                                                   n: Int, partNum : Int, offset: Int ) : Double = {

    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 1 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))
      prob += computeMKNProbWithIndexedRDDWithoutBloomFilter(mknModel, grams, index, i, partNum, offset)
    }
    math.exp((0.0 - prob)/num)
  }

  def computeMKNProbWithSQL(mknModel: MKNModel, grams: RDD[String], index: Array[RDD[(String, Long)]],
                            sc : SparkSession, pos: Int, partNum: Int) : Double = {
    val ICD = grams.map(x => (x, 1)).reduceByKey(_+_).partitionBy(new HashPartitioner(partNum)).map(elem => {
      val buffer = new ArrayBuffer[String]()
      buffer.append(elem._1)
      /**
        * tuple -> (BackOffWord, (n-1)-gram)
        */
      var tuple = (elem._1.substring(0, elem._1.lastIndexOf(" ")).trim, elem._1.substring(elem._1.indexOf(" ")).trim)
      var j = pos - 1
      while (j >= 0) {
        val tm = tuple._1 + SpecialString.mainSeparator + tuple._2
        buffer.append(tm)
        if (tuple._1.trim.split(" ").length > 1){
          val A = tuple._1.trim
          val B = tuple._2.trim
          tuple = (A.substring(A.indexOf(" ")).trim, B.substring(B.indexOf(" ")).trim)
        }
        j -= 1
      }
      (buffer.toArray, elem._2)
    }).collect()

    println((pos+1) + "-gram length " + grams.count() + " ID length " + ICD.length)
    val map = new scala.collection.mutable.HashMap[Int, (Double, Int)]
    val size = ICD.length
    for (ind <- 0 until size) {
      map += (ind -> (0.0, ICD(ind)._2))
    }
    for (j <- 0 until pos) {
      val indexedID = IndexedRDD(index(j))
      val bloomFilter = mknModel.bloomFilter.get(j)

      val probTable = sc.sqlContext.createDataFrame(mknModel.prob.get(j).map(p => Row(p._1, p._2)), Schema.GT_Schema)
      val probKey = ICD.map(x => x._1(pos - j).split(Splitter.mainSplitter)(1))
      val res = indexedID.multiget(probKey)
      val probID = probKey.map(elem => { res(elem) })
      for (ind <- 0 until size) {
        if (bloomFilter.contains(probID(ind))) {
          val count = probTable.filter(probTable("ID") === probID(ind)).collect()
          if (count.length > 0) {
            val value = map(ind)
            map += (ind -> (value._1 + count(0).getDouble(1), value._2))
          }
        }
      }
      val backoffTable = sc.sqlContext.createDataFrame(mknModel.BOW.get(j).map(p => Row(p._1, p._2)), Schema.GT_Schema)
      val backOffKey = ICD.map(x => x._1(pos - j).split(Splitter.mainSplitter)(0))
      val resBack = indexedID.multiget(backOffKey)
      val backOffID = backOffKey.map(elem => { resBack(elem) })
      for (ind <- 0 until size) {
        if (bloomFilter.contains(backOffID(ind))) {
          val count = backoffTable.filter(backoffTable("ID") === backOffID(ind)).collect()
          if (count.length > 0) {
            val value = map(ind)
            map += (ind -> (Multiply(value._1, count(0).getDouble(1)), value._2))
          }
        }
      }
    }

    val probKey = ICD.map(x => x._1(0))
    val res = IndexedRDD(index(pos)).multiget(probKey)
    val probID = probKey.map(elem => { res(elem) })
    val dataFrame = sc.sqlContext.createDataFrame(mknModel.prob.get(pos).map(p => Row(p._1, p._2)), Schema.GT_Schema)
    val bloomFilter = mknModel.bloomFilter.get(pos)
    for (ind <- 0 until size) {
      if (bloomFilter.contains(probID(ind))) {
        val count = dataFrame.filter(dataFrame("ID") === probID(ind)).collect()
        if (count.length > 0) {
          val value = map(ind)
          map += (ind -> (value._1 + count(0).getDouble(1), value._2))
        }
      }
    }

    var prob = 0.0
    var count = 0
    val probArray = map.toArray.map(x => x._2._1 * x._2._2)
    for (elem <- probArray) {
      //println(elem )
      if (elem != 0)
        prob += math.log(elem)
      else
        count += 1
    }
    println("pos  " + pos + "\tzero count "  + count + "\tprob " + prob)
    prob
  }

  def computeMKNProbWithSQLNoBloomFilter(mknModel: MKNModel, gram : RDD[String], index: Array[RDD[(String, Long)]],
                                         sc : SparkSession, pos : Int, partNum: Int) : Double = {

    val ICD = gram.map(x => (x, 1)).reduceByKey(_+_).partitionBy(new HashPartitioner(partNum)).map(elem => {
      val buffer = new ArrayBuffer[String]()
      buffer.append(elem._1)
      /**
        * tuple -> (BackOffWord, (n-1)-gram)
        */
      var tuple = (elem._1.substring(0, elem._1.lastIndexOf(" ")).trim, elem._1.substring(elem._1.indexOf(" ")).trim)
      var j = pos - 1
      while (j >= 0) {
        val tm = tuple._1 + SpecialString.mainSeparator + tuple._2
        buffer.append(tm)
        if (tuple._1.trim.split(" ").length > 1){
          val A = tuple._1.trim
          val B = tuple._2.trim
          tuple = (A.substring(A.indexOf(" ")).trim, B.substring(B.indexOf(" ")).trim)
        }
        j -= 1
      }
      (buffer.toArray, elem._2)
    }).collect()

    val map = new scala.collection.mutable.HashMap[Int, (Double, Int)]
    val size = ICD.length
    for (ind <- 0 until size) {
      map += (ind -> (0.0, ICD(ind)._2))
    }

    for (j <- 0 until pos) {
      val indexedID = IndexedRDD(index(j))

      val probTable = sc.sqlContext.createDataFrame(mknModel.prob.get(j).map(p => Row(p._1, p._2)), Schema.GT_Schema)
      val probKey = ICD.map(x => x._1(pos - j).split(Splitter.mainSplitter)(1))
      val res = indexedID.multiget(probKey)
      val probID = probKey.map(elem => { res(elem) })

      for (ind <- 0 until size) {
        val count = probTable.filter(probTable("ID") === probID(ind)).collect()
        if (count.length > 0) {
          val value = map(ind)
          map += (ind -> (value._1 + count(0).getDouble(1), value._2))
        }
      }

      val backoffTable = sc.sqlContext.createDataFrame(mknModel.BOW.get(j).map(p => Row(p._1, p._2)), Schema.GT_Schema)
      val backOffKey = ICD.map(x => x._1(pos - j).split(Splitter.mainSplitter)(0))
      val resBack = indexedID.multiget(backOffKey)
      val backOffID = backOffKey.map(elem => { resBack(elem) })
      for (ind <- 0 until size) {
        val count = backoffTable.filter(backoffTable("ID") === backOffID(ind)).collect()
        if (count.length > 0) {
          val value = map(ind)
          map += (ind -> (Multiply(value._1, count(0).getDouble(1)), value._2))
        }
      }
    }

    val probKey = ICD.map(x => x._1(0))
    val res = IndexedRDD(index(pos)).multiget(probKey)
    val probID = probKey.map(elem => { res(elem) })
    val dataFrame = sc.sqlContext.createDataFrame(mknModel.prob.get(pos).map(p => Row(p._1, p._2)), Schema.GT_Schema)
    for (ind <- 0 until size) {
      val count = dataFrame.filter(dataFrame("ID") === probID(ind)).collect()
      if (count.length > 0) {
        val value = map(ind)
        map += (ind -> (value._1 + count(0).getDouble(1), value._2))
      }
    }
    var prob = 0.0
    var count = 0
    val probArray = map.toArray.map(x => x._2._1 * x._2._2)
    for (elem <- probArray) {
      //println(elem )
      if (elem != 0)
        prob += math.log(elem)
      else
        count += 1
    }
    println("pos  " + pos + "\tzero count "  + count + "\tprob " + prob)
    prob
  }

  def computeMKNProbWithIndexedRDD(mknModel : MKNModel, gram :RDD[String], index: Array[RDD[(String, Long)]],
                                pos: Int, partNum : Int) : Double = {

    val ICD = gram.map(x => (x, 1)).reduceByKey(_+_).partitionBy(new HashPartitioner(partNum)).map(elem => {
      val buffer = new ArrayBuffer[String]()
      buffer.append(elem._1)
      /**
        * tuple -> (BackOffWord, (n-1)-gram)
        */
      var tuple = (elem._1.substring(0, elem._1.lastIndexOf(" ")).trim, elem._1.substring(elem._1.indexOf(" ")).trim)
      var j = pos - 1
      while (j >= 0) {
        val tm = tuple._1 + SpecialString.mainSeparator + tuple._2
        buffer.append(tm)
        if (tuple._1.trim.split(" ").length > 1){
          val A = tuple._1.trim
          val B = tuple._2.trim
          tuple = (A.substring(A.indexOf(" ")).trim, B.substring(B.indexOf(" ")).trim)
        }
        j -= 1
      }
      (buffer.toArray, elem._2)
    }).collect()

    val map = new scala.collection.mutable.HashMap[Int, (Double, Int)]
    val size = ICD.length
    for (ind <- 0 until size) {
      map += (ind -> (0.0, ICD(ind)._2))
    }

    for (j <- 0 until pos) {
      val indexedID = IndexedRDD(index(j))
      val bloomFilter = mknModel.bloomFilter.get(j)

      val propIndexed = IndexedRDD(mknModel.prob.get(j))
      val probKey = ICD.map(x => x._1(pos - j).split(Splitter.mainSplitter)(1))
      val res = indexedID.multiget(probKey)
      val probID = probKey.map(elem => { res(elem) })

      for (ind <- 0 until size) {
        if (bloomFilter.contains(probID(ind))) {
          val count = propIndexed.get(probID(ind))
          if (count.isDefined) {
            val value = map(ind)
            map += (ind -> (value._1 + count.get, value._2))
          }
        }
      }

      val backoffIndexed = IndexedRDD(mknModel.BOW.get(j))
      val backOffKey = ICD.map(x => x._1(pos - j).split(Splitter.mainSplitter)(0))
      val resBack = indexedID.multiget(backOffKey)
      val backOffID = backOffKey.map(elem => { resBack(elem) })
      for (ind <- 0 until size) {
        if (bloomFilter.contains(backOffID(ind))) {
          val count = backoffIndexed.get(backOffID(ind))
          if (count.isDefined) {
            val value = map(ind)
            map += (ind -> (Multiply(value._1, count.get), value._2))
          }
        }
      }
    }

    val probKey = ICD.map(x => x._1(0))
    val res = IndexedRDD(index(pos)).multiget(probKey)
    val probID = probKey.map(elem => { res(elem) })
    val lastIndex = IndexedRDD(mknModel.prob.get(pos))
    val bloomFilter = mknModel.bloomFilter.get(pos)
    for (ind <- 0 until size) {
      if (bloomFilter.contains(probID(ind))) {
        val count =lastIndex .get(probID(ind))
        if (count.isDefined) {
          val value = map(ind)
          map += (ind -> (value._1 + count.get, value._2))
        }
      }
    }
    var prob = 0.0
    var count = 0
    val probArray = map.toArray.map(x => x._2._1 * x._2._2)
    for (elem <- probArray) {
      //println(elem )
      if (elem != 0)
        prob += math.log(elem)
      else
        count += 1
    }
    println("pos  " + pos + "\tzero count "  + count + "\tprob " + prob)
    prob
  }

  def computeMKNProbWithIndexedRDDWithoutBloomFilter(mknModel : MKNModel, gram :RDD[String], index: Array[RDD[(String, Long)]],
                                                  pos: Int, partNum : Int, offset: Int) : Double = {

    val ICD = gram.map(x => (x, 1)).reduceByKey(_+_).partitionBy(new HashPartitioner(partNum)).map(elem => {
      val buffer = new ArrayBuffer[String]()
      buffer.append(elem._1)
      /**
        * tuple -> (BackOffWord, (n-1)-gram)
        */
      var tuple = (elem._1.substring(0, elem._1.lastIndexOf(" ")).trim, elem._1.substring(elem._1.indexOf(" ")).trim)
      var j = pos - 1
      while (j >= 0) {
        val tm = tuple._1 + SpecialString.mainSeparator + tuple._2
        buffer.append(tm)
        if (tuple._1.trim.split(" ").length > 1){
          val A = tuple._1.trim
          val B = tuple._2.trim
          tuple = (A.substring(A.indexOf(" ")).trim, B.substring(B.indexOf(" ")).trim)
        }
        j -= 1
      }
      (buffer.toArray, elem._2)
    }).collect()

    val map = new scala.collection.mutable.HashMap[Int, (Double, Int)]
    val size = ICD.length
    for (ind <- 0 until size) {
      map += (ind -> (0.0, ICD(ind)._2))
    }

    for (j <- 0 until pos) {

      val indexedID = IndexedRDD(index(j))
      val probKey = ICD.map(x => x._1(pos - j).split(Splitter.mainSplitter)(1))
      val res = indexedID.multiget(probKey)
      val probID = probKey.map(elem => { res(elem) })
      val propIndexed = IndexedRDD(mknModel.prob.get(j))

      var ind = 0
      while (ind < size) {
        val buffer = new ArrayBuffer[Long]()
        for (in <- ind until (ind + offset) if in < size)
          buffer.append(probID(in))

        val curMap = propIndexed.multiget(buffer.toArray)
        for (in <- ind until (ind + offset) if in < size) {
          if (curMap.contains(probID(in))) {
            val value = map(in)
            map += (in -> (value._1 + curMap(probID(in)), value._2))
          }
        }
        ind += offset
      }

      val backoffIndexed = IndexedRDD(mknModel.BOW.get(j))
      val backOffKey = ICD.map(x => x._1(pos - j).split(Splitter.mainSplitter)(0))
      val resBack = indexedID.multiget(backOffKey)
      val backOffID = backOffKey.map(elem => { resBack(elem) })

      ind = 0
      while (ind < size) {
        val buffer = new ArrayBuffer[Long]()
        for (in <- ind until (ind + offset) if in < size)
          buffer.append(backOffID(in))

        val curMap = backoffIndexed.multiget(buffer.toArray)
        for (in <- ind until (ind + offset) if in < size) {
          if (curMap.contains(backOffID(in))) {
            val value = map(in)
            map += (in -> (Multiply(value._1, curMap(backOffID(in))), value._2))
          }
        }
        ind += offset
      }
    }

    val probKey = ICD.map(x => x._1(0))
    val res = IndexedRDD(index(pos)).multiget(probKey)
    val probID = probKey.map(elem => { res(elem) })
    val lastIndex = IndexedRDD(mknModel.prob.get(pos))

    var ind = 0
    while (ind < size) {
      val buffer = new ArrayBuffer[Long]()
      for (in <- ind until (ind + offset) if in < size)
        buffer.append(probID(in))

      val curMap = lastIndex.multiget(buffer.toArray)
      for (in <- ind until (ind + offset) if in < size) {
        if (curMap.contains(probID(in))) {
          val value = map(in)
          map += (in -> (value._1 + curMap(probID(in)), value._2))
        }
      }
      ind += offset
    }

    var prob = 0.0
    var count = 0
    val probArray = map.toArray.map(x => x._2._1 * x._2._2)
    for (elem <- probArray) {
      //println(elem )
      if (elem != 0)
        prob += math.log(elem)
      else
        count += 1
    }
    println("pos  " + pos + "\tzero count "  + count + "\tprob " + prob)
    prob
  }

  def Multiply(a : Double, b : Double) : Double = {
    BIGDECIMAL.multiply(a, b)
  }
}
