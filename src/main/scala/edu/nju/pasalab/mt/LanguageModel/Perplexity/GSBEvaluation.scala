package edu.nju.pasalab.mt.LanguageModel.Perplexity

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import edu.nju.pasalab.mt.LanguageModel.SLM.StupidBackOffModel
import edu.nju.pasalab.mt.LanguageModel.util.{Schema, toolFun}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wuyan on 2016/3/30.
  */
object GSBEvaluation {
  val Backoff_Factor = 0.4

  /**
    * this function use SparkSQL to query
    *
    * @param gsbModel is a Google Stupid Backoff Model n-gram Language Model
    * @param testSet is the test Set for evaluation Good Turing n-gram Language Model
    * @param index an unify indexing for word sequence of n-gram
    * @param n is the value the N
    * @param sc is the SparkSQL environment
    * @return the Perplexity of Good Turing n-gram Language Model
    */
  def computeGSBPerplexitySQL(gsbModel : StupidBackOffModel, testSet : RDD[String], index: Array[RDD[(String, Long)]],
                               n: Int,sc : SparkSession) : Double = {

    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 0 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))
      prob += computeGSBProbSQL(gsbModel, grams, index, sc, i)
      //println(i + "\t" + prob)
    }
    math.exp((0.0 - prob)/num)
  }

  def computeGSBPerplexitySQL_NOBloom(gsbModel : StupidBackOffModel, testSet : RDD[String], index: Array[RDD[(String, Long)]],
                                  n: Int, sc : SparkSession) : Double = {
    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 0 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))
      prob += computeGSBProbSQL_NOBloom(gsbModel, grams, index, sc, i)
    }
    math.exp((0.0 - prob)/num)
  }

  def computeGSBPerplexityIndexedRDD(gsbModel : StupidBackOffModel, testSet : RDD[String], index: Array[RDD[(String, Long)]],
                                     n: Int) : Double = {
    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 0 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))
      prob += computeGSBProbIndexedRDD(gsbModel, grams, index, i)
      //println(i + "\t" + prob)
    }
    math.exp((0.0 - prob)/num)
  }

  def computeGSBPerplexityIndexedRDD_NOBloom(gsbModel : StupidBackOffModel, testSet : RDD[String], index: Array[RDD[(String, Long)]],
                                     n: Int, offset: Int) : Double = {
    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).count() * 1.0
    val arr = testSet.flatMap(x => toolFun.getSentence(x, n))
    var prob = 0.0
    for (i <- 0 until n) {
      val grams = arr.filter(x => x.split(" ").length == (i+1))
      prob += computeGSBProbIndexedRDDNOBloom(gsbModel, grams, index,i, offset)
    }
    math.exp((0.0 - prob)/num)
  }

  def computeGSBProbSQL(gsbModel :StupidBackOffModel, gram : RDD[String], indexing : Array[RDD[(String, Long)]],
                        sc : SparkSession, pos : Int) : Double = {
    var prob = 0.0
    if (pos == 0) {
      val keyID = gram.map(x => (x, 1)).reduceByKey(_+_).join(indexing(pos)).map(elem => (elem._2._2, elem._2._1)).collect()
      val probIndexed = IndexedRDD(gsbModel.prob.get(pos))
       prob += math.log(probIndexed.get(keyID(0)._1).get * keyID(0)._2)
    } else {
      val id = gram.map(x => (x, 1)).reduceByKey(_+_).map(elem => {
        val buffer = new Array[String](pos+1)

        var k = 0
        buffer(k) = elem._1
        k += 1
        var tmp = if(elem._1.trim.split(" ").length > 1) Some(elem._1.substring(elem._1.indexOf(" "))) else None

        while (k <= pos) {
          val str = tmp.get.trim
          buffer(k) = str
          tmp = if(str.trim.split(" ").length > 1) Some(str.substring(str.indexOf(" "))) else None
          k += 1
        }
        (buffer, elem._2)
      }).collect()


      println((pos+1) + "-gram length " + gram.count() + " ID length " + id.length)
      val map = new scala.collection.mutable.HashMap[Int, (Double, Int)]
      val size = id.length
      for (ind <- 0 until size) {
        map += (ind -> (0.0, id(ind)._2))
      }

      var i = pos
      val key = id.map(x => x._1(pos - i))
      val dataFrame = sc.sqlContext.createDataFrame(gsbModel.prob.get(i).map(p => Row(p._1, p._2)), Schema.GT_Schema)
      val bloomFilter = gsbModel.bloomFilter.get(i)
      val indexedID = IndexedRDD(indexing(i))
      val res = indexedID.multiget(key)
      val keyID = key.map(elem => { res(elem)})

      var curMap = new scala.collection.mutable.HashMap[Int, String]

      for (ind <- 0 until size) {
        if (bloomFilter.contains(keyID(ind))) {
          val count = dataFrame.filter(dataFrame("ID") === keyID(ind)).collect()
          if (count.length > 0) {
            val value = map(ind)
            map += (ind -> (math.log(count(0).getDouble(1)), value._2))
          } else {
            curMap += (ind -> id(ind)._1(pos - i + 1))
          }
        } else {
          if ((pos - i + 1) <= pos)
            curMap += (ind -> id(ind)._1(pos - i + 1))
        }
      }

      i -= 1
      while (i >= 0) {
        if (curMap.nonEmpty) {
          println(i + "\tcurmap size " + curMap.size)
          val curKey = curMap.toArray.map(x => x._2)
          val dataFrame = sc.sqlContext.createDataFrame(gsbModel.prob.get(i).map(p => Row(p._1, p._2)), Schema.GT_Schema)
          val bloomFilter = gsbModel.bloomFilter.get(i)
          val indexedID = IndexedRDD(indexing(i))
          val res = indexedID.multiget(curKey)
          val curID = curMap.map(elem => { (elem._1, res(elem._2)) })

          val MAP = new scala.collection.mutable.HashMap[Int, String]
          for ((k, v) <- curID) {
            if (bloomFilter.contains(v)) {
              val count = dataFrame.filter(dataFrame("ID") === v).collect()
              if (count.length > 0) {
                val value = map(k)
                map += (k -> (math.log(Backoff_Factor * count(0).getDouble(1)) , value._2))
              } else {
                if ((pos - i + 1) <= pos)
                  MAP += (k -> id(k)._1(pos - i + 1))
              }
            } else {
              if ((pos - i + 1) <= pos)
                MAP += (k -> id(k)._1(pos - i + 1))
            }
          }
          curMap = MAP
        }
        i -= 1
      }
      var count = 0
      val probArray = map.toArray.map(x => x._2._1 + math.log(x._2._2))
      for (elem <- probArray) {
        prob += elem
        if (elem == 0.0)
          count += 1
      }
      println("pos  " + pos + "\tzero count "  + count)
    }
    prob
  }

  def computeGSBProbSQL_NOBloom(gsbModel :StupidBackOffModel, gram : RDD[String], indexing : Array[RDD[(String, Long)]],
                                sc : SparkSession, pos : Int) : Double = {
    var prob = 0.0
    if (pos == 0) {
      val keyID = gram.map(x => (x, 1)).reduceByKey(_+_).join(indexing(pos)).map(elem => (elem._2._2, elem._2._1)).collect()
      val probIndexed = IndexedRDD(gsbModel.prob.get(pos))
      prob += math.log(probIndexed.get(keyID(0)._1).get * keyID(0)._2)
    } else {
      val id = gram.map(x => (x, 1)).reduceByKey(_+_).map(elem => {
        val buffer = new Array[String](pos+1)

        var k = 0
        buffer(k) = elem._1
        k += 1
        var tmp = if(elem._1.trim.split(" ").length > 1) Some(elem._1.substring(elem._1.indexOf(" "))) else None

        while (k <= pos) {
          val str = tmp.get.trim
          buffer(k) = str
          tmp = if(str.trim.split(" ").length > 1) Some(str.substring(str.indexOf(" "))) else None
          k += 1
        }
        (buffer, elem._2)
      }).collect()

      //println((pos+1) + "-gram length " + gram.count() + " ID length " + id.length)
      val map = new scala.collection.mutable.HashMap[Int, (Double, Int)]
      val size = id.length
      for (ind <- 0 until size) {
        map += (ind -> (0.0, id(ind)._2))
      }

      var i = pos
      val key = id.map(x => x._1(pos - i))
      val dataFrame = sc.sqlContext.createDataFrame(gsbModel.prob.get(i).map(p => Row(p._1, p._2)), Schema.GT_Schema)
      val indexedID = IndexedRDD(indexing(i))
      val res = indexedID.multiget(key)
      val keyID = key.map(elem => { res(elem)})

      var curMap = new scala.collection.mutable.HashMap[Int, String]

      for (ind <- 0 until size) {
        val count = dataFrame.filter(dataFrame("ID") === keyID(ind)).collect()
        if (count.length > 0) {
          val value = map(ind)
          map += (ind -> (math.log(count(0).getDouble(1)), value._2))
        } else {
          if ((pos - i + 1) <= pos)
            curMap += (ind -> id(ind)._1(pos - i + 1))
        }
      }

      i -= 1

      while (i >= 0) {
        if (curMap.nonEmpty) {
          println(i + "\tcurmap size " + curMap.size)
          val curKey = curMap.toArray.map(x => x._2)
          val dataFrame = sc.sqlContext.createDataFrame(gsbModel.prob.get(i).map(p => Row(p._1, p._2)), Schema.GT_Schema)
          val indexedID = IndexedRDD(indexing(i))
          val res = indexedID.multiget(curKey)
          val curID = curMap.map(elem => { (elem._1, res(elem._2)) })

          val MAP = new scala.collection.mutable.HashMap[Int, String]
          for ((k, v) <- curID) {
            val count = dataFrame.filter(dataFrame("ID") === v).collect()
            if (count.length > 0) {
              val value = map(k)
              map += (k -> (math.log(Backoff_Factor * count(0).getDouble(1)), value._2))
            } else {
              if ((pos - i + 1) <= pos) {
                MAP += (k -> id(k)._1(pos - i + 1))
              }
            }
          }
          curMap = MAP
        }
        i -= 1
      }
      var count = 0
      val probArray = map.toArray.map(x => x._2._1 + math.log(x._2._2))
      for (elem <- probArray) {
        prob += elem
        if (elem == 0.0)
          count += 1
      }
      println("pos  " + pos + "\tzero count "  + count)
    }
    prob
  }



  def computeGSBProbIndexedRDD(gsbModel :StupidBackOffModel, gram : RDD[String],
                               indexing : Array[RDD[(String, Long)]], pos : Int) : Double = {

    var prob = 0.0
    if (pos == 0) {
      val keyID = gram.map(x => (x, 1)).reduceByKey(_+_).join(indexing(pos)).map(elem => (elem._2._2, elem._2._1)).collect()
      val probIndexed = IndexedRDD(gsbModel.prob.get(pos))
      prob += math.log(probIndexed.get(keyID(0)._1).get * keyID(0)._2)
    } else {
      val id = gram.map(x => (x, 1)).reduceByKey(_+_).map(elem => {
        val buffer = new Array[String](pos+1)

        var k = 0
        buffer(k) = elem._1
        k += 1
        var tmp = if(elem._1.trim.split(" ").length > 1) Some(elem._1.substring(elem._1.indexOf(" "))) else None

        while (k <= pos) {
          val str = tmp.get.trim
          buffer(k) = str
          tmp = if(str.trim.split(" ").length > 1) Some(str.substring(str.indexOf(" "))) else None
          k += 1
        }
        (buffer, elem._2)
      }).collect()

     // println((pos+1) + "-gram length " + gram.count() + " ID length " + id.length)
      val map = new scala.collection.mutable.HashMap[Int, (Double, Int)]
      val size = id.length
      for (ind <- 0 until size) {
        map += (ind -> (0.0, id(ind)._2))
      }

      var i = pos
      val key = id.map(x => x._1(pos - i))
      val probIndexed = IndexedRDD(gsbModel.prob.get(i))
      val bloomFilter = gsbModel.bloomFilter.get(i)
      val indexedID = IndexedRDD(indexing(i))
      val res = indexedID.multiget(key)
      val keyID = key.map(elem => { res(elem)})

      var curMap = new scala.collection.mutable.HashMap[Int, String]

      for (ind <- 0 until size) {
        if (bloomFilter.contains(keyID(ind))) {
          val count = probIndexed.get(keyID(ind))
          if (count.isDefined) {
            val value = map(ind)
            map += (ind -> (math.log(count.get),  value._2))
          } else {
            if ((pos - i + 1) <= pos)
              curMap += (ind -> id(ind)._1(pos - i + 1))
          }
        } else {
          if ((pos - i + 1) <= pos)
            curMap += (ind -> id(ind)._1(pos - i + 1))
        }
      }

      i -= 1
      while (i >= 0) {
        if (curMap.nonEmpty) {
          println(i + "\tcurmap size " + curMap.size)
          val curKey = curMap.toArray.map(x => x._2)
          val probIndexed = IndexedRDD(gsbModel.prob.get(i))
          val bloomFilter = gsbModel.bloomFilter.get(i)
          val indexedID = IndexedRDD(indexing(i))
          val res = indexedID.multiget(curKey)
          val curID = curMap.map(elem => { (elem._1, res(elem._2)) })

          val MAP = new scala.collection.mutable.HashMap[Int, String]
          for ((k, v) <- curID) {
            if (bloomFilter.contains(v)) {
              val count = probIndexed.get(v)
              if (count.isDefined) {
                val value = map(k)
                map += (k -> (math.log(Backoff_Factor * count.get), value._2))
              } else {
                if ((pos - i + 1) <= pos)
                  MAP += (k -> id(k)._1(pos - i + 1))
              }
            } else {
              if ((pos - i + 1) <= pos)
                MAP += (k -> id(k)._1(pos - i + 1))
            }
          }
          curMap = MAP
        }
        i -= 1
      }

      var count = 0
      val probArray = map.toArray.map(x => x._2._1 + math.log(x._2._2))
      for (elem <- probArray) {
        prob += elem
        if (elem == 0.0)
          count += 1
      }
      println("pos  " + pos + "\tzero count "  + count)
    }
    prob
  }

  def computeGSBProbIndexedRDDNOBloom(gsbModel :StupidBackOffModel, gram : RDD[String],
                               indexing : Array[RDD[(String, Long)]], pos : Int, offset : Int) : Double = {
    var prob = 0.0
    if (pos == 0) {
      val keyID = gram.map(x => (x, 1)).reduceByKey(_+_).join(indexing(pos)).map(elem => (elem._2._2, elem._2._1)).collect()
      val probIndexed = IndexedRDD(gsbModel.prob.get(pos))
      prob += math.log(probIndexed.get(keyID(0)._1).get * keyID(0)._2)
    } else {
      val id = gram.map(x => (x, 1)).reduceByKey(_+_).map(elem => {
        val buffer = new Array[String](pos+1)

        var k = 0
        buffer(k) = elem._1
        k += 1
        var tmp = if(elem._1.trim.split(" ").length > 1) Some(elem._1.substring(elem._1.indexOf(" "))) else None

        while (k <= pos) {
          val str = tmp.get.trim
          buffer(k) = str
          tmp = if(str.trim.split(" ").length > 1) Some(str.substring(str.indexOf(" "))) else None
          k += 1
        }
        (buffer, elem._2)
      }).collect()


      val map = new scala.collection.mutable.HashMap[Int, (Double, Int)]
      val size = id.length
      for (ind <- 0 until size) {
        map += (ind -> (0.0, id(ind)._2))
      }

      var i = pos
      val key = id.map(x => x._1(pos - i))
      val probIndexed = IndexedRDD(gsbModel.prob.get(i))
      val indexedID = IndexedRDD(indexing(i))
      val res = indexedID.multiget(key)
      val keyID = key.map(elem => { res(elem)})

      var curMap = new scala.collection.mutable.HashMap[Int, String]
      var ind = 0
      while (ind < size) {
        val buffer = new ArrayBuffer[Long]()
        for (in <- ind until (ind + offset) if in < size)
          buffer.append(keyID(in))

        val result = probIndexed.multiget(buffer.toArray)
        for (in <- ind until (ind + offset) if in < size) {
          if (result.contains(keyID(in))) {
            val value = map(in)
            map += (in -> (math.log(result(keyID(in))), value._2))
          } else {
            if ((pos - i + 1) <= pos)
              curMap += (in -> id(in)._1(pos - i + 1))
          }
        }

        ind += offset
      }
      i -= 1

      while (i >= 0) {
        if (curMap.nonEmpty) {
          println(i + "\tcurmap size " + curMap.size)
          val curKey = curMap.toArray.map(x => x._2)
          val probIndexed = IndexedRDD(gsbModel.prob.get(i))
          val indexedID = IndexedRDD(indexing(i))
          val res = indexedID.multiget(curKey)
          val curID = curMap.map(elem => { (elem._1, res(elem._2)) })

          val MAP = new scala.collection.mutable.HashMap[Int, String]

          val WT = probIndexed.multiget(curID.values.toArray)

          for ((k, v) <- curID) {
            if (WT.contains(v)) {
              val value = map(k)
              map += (k -> (math.log(Backoff_Factor * WT(v)), value._2))
            } else {
              if ((pos - i + 1) <= pos) {
                MAP += (k -> id(k)._1(pos - i + 1))
              }
            }
          }
          curMap = MAP
        }
        i -= 1
      }
      var count = 0
      val probArray = map.toArray.map(x => x._2._1 + math.log(x._2._2))
      for (elem <- probArray) {
        prob += elem
        if (elem == 0.0)
          count += 1
      }
      println("pos  " + pos + "\tzero count "  + count)
    }
    prob
  }
}
