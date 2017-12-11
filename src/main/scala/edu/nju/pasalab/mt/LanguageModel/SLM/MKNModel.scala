package edu.nju.pasalab.mt.LanguageModel.SLM

import breeze.util.BloomFilter
import edu.nju.pasalab.mt.LanguageModel.Smoothing.{KNSmoothing, MKNSmoothing}
import edu.nju.pasalab.mt.LanguageModel.util.{NGram, SpecialString, Splitter}
import edu.nju.pasalab.mt.util.{SntToCooc, CommonFileOperations}
import edu.nju.pasalab.util.math.basic
import edu.nju.pasalab.util.{gramPartitioner, ExtractionParameters}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.collection._
import scala.collection.mutable.ArrayBuffer

/**
  * This class is build a Modified Kneser-Ney Smoothing n-gram Language Model
  * In Query layer we add BloomFilter
  * use Spark SQL and IndexedRDD query respectively.respectively
  * Created by wuyan on 2016/3/22.
  */

class MKNModel extends Serializable{
  var N = 0
  var prob : Option[Array[RDD[(Long, Double)]]] = None
  var BOW : Option[Array[RDD[(Long, Double)]]] = None
  var bloomFilter :Option[Array[BloomFilter[Long]]] = None

  /**
    * construction function for Kneser-Ney Model
    *
    * @param sc is SparkContext
    * @param trainData is the trainSet
    * @param count n-gram numbers
    * @param ep parameters
    */
  def this(sc: SparkSession, trainData : Array[RDD[(String, Int)]], count : Array[Long], ep: ExtractionParameters) {
    this()
    this.N = ep.N
    this.compute(sc, trainData, count, ep)
    //this.set(sc, trainData, index, ep.partitionNum)
    //this.setBloomFilter(this.prob.get,this.BOW.get, ep.expectedErrorRate)
  }

  /**
    * this function is aimed to initialize prob and BOW
    * for Largest N-gram proBo is organized as follow:  (ID, P*)
    * for the rest of (N-1)-gram proBO is organized as follow: (ID, P*, BOW),
    * Note P* is the front part of Modified Kneser-Ney function
    *      BOW means Backoff weights
    *
    * @param sc is SparkContext
    * @param grams n-grams
    * @param count grams number array
    * @param ep parameters
    */
  def compute (sc : SparkSession, grams : Array[RDD[(String, Int)]], count : Array[Long], ep : ExtractionParameters): Unit = {
    CommonFileOperations.deleteIfExists(ep.lmRootDir + "/mkn")
    val dictFile = sc.sparkContext.textFile(SntToCooc.getLMDictPath(ep.lmRootDir)).filter(_.length > 0).map(line => {
      val sp = line.trim.split("\\s+")
      (sp(0), sp(1).toInt)
    })
    val dict : Int2ObjectOpenHashMap[String] = new Int2ObjectOpenHashMap[String]
    for (elem <- dictFile.collect()) {
      dict.put(elem._2, elem._1)
    }
    val idBroad = sc.sparkContext.broadcast(dict)
    val D = getDArray(grams, ep.N)

    var pre_gram = grams(0).mapPartitions(part => {
      part.map(elem =>{
        (elem._1, Divide(elem._2, count(0)))
      })
    })

    for (i <- 1 until ep.N) {
      val prop = simpleMap(grams(i).repartitionAndSortWithinPartitions(new gramPartitioner(ep.partitionNum)))
        //.partitionBy(new gramPartitioner(ep.partitionNum)))
      val part1 = getAllCounts(prop, D(i))

      val adjusted = part1.map(elem => {
        val tmp = elem._1.trim.substring(0, elem._1.trim.lastIndexOf(" "))
        (tmp, elem)
      }).join(pre_gram).map(elem => {
        val value = elem._2._1._2 + elem._2._1._3 * elem._2._2
        (elem._2._1._1, value)
      })

      val BOW = part1.map(elem => {
        val sp = elem._1.trim.substring(0, elem._1.trim.lastIndexOf(" "))
        (sp, elem._3)
      }).distinct()

      if (i == 1) {
        println(i + "\t" + pre_gram.count() + "\t" + BOW.count() + "\t")

        val gram = pre_gram.join(BOW).mapPartitions(part => {
          val id = idBroad.value
          part.map(elem => {
            val sb = new StringBuilder(128)
            val tmp = elem._1.toInt
            if (tmp == 0){
              sb.append(0.0).append("\t").append("<s>").append("\t").append(Math.log10(elem._2._2))
            } else {
              sb.append(Math.log10(elem._2._1)).append("\t").append(id.get(tmp)).append("\t")
              if (tmp == 1){
                sb.append(0.0)
              } else {
                sb.append(Math.log10(elem._2._2))
              }
            }
            sb.toString()
          })
        })
        if (gram.count() == count(i-1)){
          gram.union(sc.sparkContext.makeRDD(Array(basic.Divide(1.0, count(i)) + "\t" + "unk\t 0.0")))
            .repartition(1).saveAsTextFile(SntToCooc.getMKNGramPath(ep.lmRootDir, i))
        } else{
          gram.union(
            sc.sparkContext.makeRDD(
              Array(basic.Divide(1.0, count(i)) + "\tunk\t 0.0", Math.log10(0.5) + "\t</s>\t0.0"))
          ).repartition(ep.partitionNum).saveAsTextFile(SntToCooc.getMKNGramPath(ep.lmRootDir, i))
        }

      } else {
        BOW.repartitionAndSortWithinPartitions(new HashPartitioner(ep.partitionNum))
          //.partitionBy(new HashPartitioner(ep.partitionNum))
        val before = pre_gram.map(_._1).subtract(BOW.map(_._1)).map(elem => (elem, 1))
          .partitionBy(new HashPartitioner(ep.partitionNum))

        println(i + "\t" + pre_gram.count() + "\t"  + BOW.count() + "\t res pair number : " + before.count())

        val res = pre_gram.join(before).mapPartitions(part =>{
          val id = idBroad.value
          part.map(elem => {
            val sp = new StringBuilder(128)
            val split = elem._1.trim.split("\\s+")
            sp.append(Math.log10(elem._2._1)).append("\t").append(id.get(split(0).toInt))
            for (i <- 1 until split.length)
              sp.append(" ").append(id.get(split(i).toInt))
            sp.append("\t").append(Math.log10(1))
            sp.toString()
          })
        })

        val gram = pre_gram.join(BOW).mapPartitions(part => {
          val id = idBroad.value
          part.map(elem =>{
            val sp = new StringBuilder(128)
            val split = elem._1.trim.split("\\s+")

            sp.append(Math.log10(elem._2._1)).append("\t").append(id.get(split(0).toInt))
            for (i <- 1 until split.length)
              sp.append(" ").append(id.get(split(i).toInt))
            sp.append("\t").append(Math.log10(elem._2._2))
            sp.toString()
          })
        }).union(res)

        gram.repartition(ep.partitionNum).saveAsTextFile(SntToCooc.getMKNGramPath(ep.lmRootDir, i))
      }

      pre_gram = adjusted
        .repartitionAndSortWithinPartitions(new HashPartitioner(ep.partitionNum))
        //.partitionBy(new HashPartitioner(ep.partitionNum))
    }
    pre_gram.mapPartitions(part =>{
      val id = idBroad.value
      part.map(elem =>{
        val sp = new StringBuilder(128)
        val split = elem._1.trim.split("\\s+")
        sp.append(Math.log10(elem._2)).append("\t").append(id.get(split(0).toInt))
        for (i <- 1 until split.length)
          sp.append(" ").append(id.get(split(i).toInt))
        sp.toString()
      })
    }).saveAsTextFile(SntToCooc.getMKNGramPath(ep.lmRootDir, ep.N))
  }

  def simpleMap(data : RDD[(String, Int)]) : RDD[(String, (String, Int))] ={
    val prop = data.map(elem => {
      val tmp = elem._1.substring(0, elem._1.lastIndexOf(" "))
      (tmp, (elem._1, elem._2))
    })
    prop
  }

  def getAllCounts(data: RDD[(String, (String, Int))], D : Array[Double]): RDD[(String, Double, Double)] = {
    val res = data.groupByKey().map(elem => {
      val arr = elem._2.toArray
      val sum : Long = arr.map(_._2).sum
      val fresh :Array[Int] = new Array[Int](3)
      val num = arr.map(elem => {
        val sp = elem._1.trim.indexOf(" ")
        elem._1.trim.substring(sp + 1)
      }).distinct.map(x => (x, 1)).groupBy(x=> x._1).mapValues(elem => {
        var sum = 0
        for (x <- elem)
          sum += x._2
        sum
      })

      var n1 : Int = 0
      var n2 : Int = 0
      var n3 : Int = 0
      for ((k, v) <- num) {
        v match {
          case 1 => n1 += 1
          case 2 => n2 += 1
          case _ => n3 += 1
        }
      }
      /*if (n1 > 0) fresh(0) = n1 else fresh(0) = 1
      if (n2 > 0) fresh(1) = n2 else fresh(1) = 1
      if (n3 > 0) fresh(2) = n3 else fresh(2) = 1*/
      fresh(0) = n1
      fresh(1) = n2
      fresh(2) = n3
      val buffer = new ArrayBuffer[(String, Double, Double)]()
      for (elem <- arr) {
        buffer.append((elem._1,
          SmoothedProbability(elem._2, sum, D),
          getBackOffWeights(sum, fresh, D)))
      }
      buffer.toArray[((String, Double, Double))]
    }).flatMap(x => x)
    res
  }

  def getKNSmoothing(grams :Array[RDD[(String, Int)]], N: Int): Array[MKNSmoothing] = {
    val kn = new Array[MKNSmoothing](N)
    for (i <- 0 until N) {
      kn(i) = new MKNSmoothing(grams(i).map(x => (x._2, 1)).reduceByKey(_+_))
    }
    kn
  }

  def getDArray(grams :Array[RDD[(String, Int)]], N: Int) : Array[Array[Double]] = {
    val D = new Array[Array[Double]](N)
    for (i <- 0 until N) {
      val mkn = new MKNSmoothing(grams(i).map(x => (x._2, 1)).reduceByKey(_+_))
      D(i) = mkn.getD
    }
    D
  }

  def SmoothedProbability(count : Long, prefix_count : Long,  arr: Array[Double]): Double = {
    val D = if (count == 1)
      arr(0)
    else if (count == 2)
      arr(1)
    else if (count == 3)
      arr(2)
    else
      0.0
    val max = Math.max(count * 1.0 - D , 0)
    val result = if (max > 0 && prefix_count != 0)
      max / (prefix_count * 1.0)
    else
      0.0
    result
  }

  def getBackOffWeights(count : Long, fresh : Array[Int], D : Array[Double]) : Double = {
    var molecular = 0.0
    for (i <- 0 until 3)
      molecular += D(i) * fresh(i)
    var result = 0.0
    if (molecular > 0) {
      result = molecular / (count * 1.0)
    }
    result
  }

  def Divide(a : Int, b: Long) : Double ={
    (a * 1.0) / (b * 1.0)
  }

  /**
    * this function is aimed to initialize prob and BOW
    * for Largest N-gram proBo is organized as follow:  (ID, P*)
    * for the rest of (N-1)-gram proBO is organized as follow: (ID, P*, BOW),
    * Note P* is the front part of Modified Kneser-Ney function
    *      BOW means Backoff weights
    *
    * @param sc is SparkContext
    * @param trainData is the trainSet
    * @param index an unify indexing for word sequence of n-gram
    * @param partNum is the value of partition number
    */
  def set(sc : SparkSession, trainData : RDD[String], index: Array[RDD[(String, Long)]], partNum : Int): Unit ={

    val PROB = new Array[RDD[(Long, Double)]](N)
    val backOffWeights = new Array[RDD[(Long, Double)]](N)

    var i = N - 1
    var grams = NGram.get_NGram(trainData, N).partitionBy(new HashPartitioner(partNum)).cache()
    val MKN = new MKNSmoothing(grams.map(x => (x._2, 1)).reduceByKey(_+_))
    var D = MKN.getD
    val prop = simpleMap(grams)
    val indexing = index(i)

    PROB(i) = prop
      .join(prop.map(x => (x._1, x._2._2)).reduceByKey(_+_), partNum)
      .map(elem => { (elem._2._1._1, SmoothedProbability(elem._2._1._2, elem._2._2, D)) })
      .join(indexing, partNum)
      .map(elem => (elem._2._2, elem._2._1))


    i -= 1
    if (i == 0) {
      val lastIndex = index(i)

      backOffWeights(i) = getBackOffInfo(grams)
        .map(elem => {(elem._1, getBackOffWeights(elem._2._1, elem._2._2, D))})
        .join(lastIndex, partNum)
        .map(elem => (elem._2._2, elem._2._1))

      val count = grams.count()
      PROB(i) = getProbabilityOfUnigram(grams.map(x => x._1), count)
        .join(lastIndex)
        .map(elem => (elem._2._2, elem._2._1))
    }

    while (i > 0) {
      //println("----------------------------------------------- Start " + i + " round --------------------------------------------------------------")
      val indexing = index(i)

      backOffWeights(i) = getBackOffInfo(grams)
        .map(elem => {(elem._1, getBackOffWeights(elem._2._1, elem._2._2, D))})
        .join(indexing)
        .map(elem => (elem._2._2, elem._2._1))

      grams.unpersist()

      val NextGrams = NGram.get_NGram(trainData, i+1).partitionBy(new HashPartitioner(partNum)).cache()
      val MKN = new MKNSmoothing(NextGrams.map(x => (x._2, 1)).reduceByKey(_+_))
      val prefix_D = MKN.getD
      val NextProp = simpleMap(NextGrams)

      PROB(i) = NextProp
        .join(NextProp.map(x => (x._1, x._2._2)).reduceByKey(_+_), partNum)
        .map(elem => { (elem._2._1._1, SmoothedProbability(elem._2._1._2, elem._2._2, prefix_D)) })
        .join(indexing, partNum)
        .map(elem => (elem._2._2, elem._2._1))

      if (i == 1) {
        val lastIndex = index(i-1)

        backOffWeights(i-1) = getBackOffInfo(NextGrams)
          .map(elem => {(elem._1, getBackOffWeights(elem._2._1, elem._2._2, prefix_D))})
          .join(lastIndex)
          .map(elem => (elem._2._2, elem._2._1))

        val count = NextGrams.count()
        PROB(i-1) = getProbabilityOfUnigram(grams.map(x => x._1), count)
          .join(lastIndex)
          .map(elem => (elem._2._2, elem._2._1))

        NextGrams.unpersist()
      } else {
        grams = NextGrams
        D = prefix_D
      }
      //println("----------------------------------------------- Done " + i + "round --------------------------------------------------------------")
      i -= 1
    }

    prob = Some(PROB)
    BOW = Some(backOffWeights)
  }

  /** This method is aimed at to compute the coefficient about MKN Backoff weights
    * first is prefix_count
    * Next is :
    *         N1(Wi-n+1 ~ Wi-1 *)=|{wi:C(Wi-n+1 ~ Wi) = 1}|
    *         N2(Wi-n+1 ~ Wi-1 *)=|{wi:C(Wi-n+1 ~ Wi) = 2}|
    *         N3+(Wi-n+1 ~ Wi-1 *)=|{wi:C(Wi-n+1 ~ Wi) > 2}|
    *
    * @param gram : gram is the n-gram
    * @return
    *         (prefix_str, (prefix_count, Array(N1, N2, N3+)))
    */
  def getBackOffInfo(gram : RDD[(String, Int)]) : RDD[(String, (Int, Array[Int]))] = {
    val num = gram.map(elem => {
      val tmp = elem._1.substring(0, elem._1.lastIndexOf(" "))
      val arr: Array[Int] = new Array[Int](3)
      if (elem._2 == 1L)
        arr(0) = 1
      else if(elem._2 == 2L)
        arr(1) = 1
      else if(elem._2 > 2L)
        arr(2) = 1
      (tmp, (elem._2, arr))
    }).reduceByKey((a, b) => {
      for (i <- 0 until 3)
        a._2(i) += b._2(i)
      (a._1 + b._1, a._2)
    })
    num
  }

  /**
    * This function is aimed at getting the following value:
    * N1+(.Wi) = |{Wi-1: C(Wi-1 Wi} > 0| is the number of different words that precede wi in the training data
    * N1+(..) is total sum of N1+(.Wj)
    * So for Pkn(Wi):
    *              Pkn(Wi) = N1+(.Wi) / N1+(..)
    *
    * @param gram2 : bigram for training data
    * @param count  : is the total number of bigram
    * @return : the Probability of unigram for KN Smoothing
    */
  def getProbabilityOfUnigram(gram2 : RDD[String], count : Long) :RDD[(String, Double)] = {
    val num = gram2.map(elem => {
      val split = elem.split(" ")
      (split(1), elem)
    }).reduceByKey((a, b) => {
      val sb = new StringBuilder(128)
      sb.append(a).append(SpecialString.mainSeparator).append(b)
      sb.toString()
    }).map(elem => {
      val split = elem._2.split(Splitter.mainSplitter).map(x => x.split(" "))
      val set  = new mutable.HashSet[String]()
      for (e <- split) {
        set.add(e(0))
      }
      (elem._1, set.size)
    }).mapValues(x => Divide(x, count))
    num
  }

  /**
    * this function is initialize bloomFilter
    *
    * @param probA is the ID for n-grams
    * @param bow  is the backoff weights
    * @param p is the value the expected error rate for BloomFilter
    */
  def setBloomFilter(probA : Array[RDD[(Long, Double)]], bow : Array[RDD[(Long, Double)]] , p : Double): Unit = {
    val filter : Array[BloomFilter[Long]] = new Array[BloomFilter[Long]](N)
    for (i <- 0 until N) {
      var setA : Option[Array[Long]] = None
      if (i == (N-1)) {
        setA = Some(probA(i).map(x => x._1).collect())
      } else {
        setA = Some(probA(i).map(x => x._1).union(bow(i).map(x => x._1)).collect().distinct)
      }
      val set = setA.get
      val (m, k) = BloomFilter.optimalSize(set.length, p)
      val bloom = new BloomFilter[Long](m ,k, new java.util.BitSet)
      set.map(x => bloom.+=(x))
      filter(i) = bloom
    }
    bloomFilter = Some(filter)
  }
}