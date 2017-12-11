package edu.nju.pasalab.mt.LanguageModel.SLM

import breeze.util.BloomFilter
import edu.nju.pasalab.mt.LanguageModel.Smoothing.KNSmoothing
import edu.nju.pasalab.mt.LanguageModel.util._
import edu.nju.pasalab.mt.util.{CommonFileOperations, SntToCooc}
import edu.nju.pasalab.util.math.{basic, BIGDECIMAL}
import edu.nju.pasalab.util.{gramPartitioner, ExtractionParameters}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection._

import scala.collection.mutable.ArrayBuffer

/**
  * This class is build a  Kneser-Ney Smoothing n-gram language model.
  * In Query layer we add BloomFilter
  * use Spark SQL and IndexedRDD query respectively.
  * for special :
  * 1-gram we add three special token : <s> </s> <unk>, we mark:
  * 0 <s> backoff
  * p </s> 0
  * p <unk> 0
  *
  *at this level  0 means : log10(Infinite negative integer)
  * Created by wuyan on 2016/3/22.
  */
class KNModel extends Serializable {
  val preserve = true
  var N = 0

  var prob : Option[Array[RDD[(Long, Double)]]] = None
  var BOW :  Option[Array[RDD[(Long, Double)]]] = None
  var bloomFilter :Option[Array[BloomFilter[Long]]] = None

  /**
    * construction function for Kneser-Ney Model
    *
    * @param sc is SparkContext
    * @param trainData is the trainSet
    * @param ep parameters
    */
  def this(sc: SparkSession, trainData : Array[RDD[(String, Int)]], count : Array[Long], ep: ExtractionParameters) {
    this()
    this.N = N
    this.compute(sc, trainData, count, ep)
    //this.set(sc, trainData, index, ep)
    //this.setBloomFilter(this.prob.get, this.BOW.get,  p)
  }

  /**
    * this function is aimed to initialize proBO
    * for Largest N-gram proBo is organized as follow:  (ID, P*)
    * for the rest of (N-1)-gram proBO is organized as follow: (ID, P*, BOW),
    * Note P* is the front part of Kneser-Ney function
    *      BOW means Backoff weights
    *
    * @param sc is SparkContext
    * @param grams is the trainSet
    * @param ep is the parameters
    */

  def compute (sc : SparkSession, grams : Array[RDD[(String, Int)]], count : Array[Long], ep : ExtractionParameters): Unit = {
    CommonFileOperations.deleteIfExists(ep.lmRootDir + "/kn")
    val dictFile = sc.sparkContext.textFile(SntToCooc.getLMDictPath(ep.lmRootDir)).filter(_.length > 0).map(line => {
      val sp = line.trim.split("\\s+")
      (sp(0), sp(1).toInt)
    })
    val dict : Int2ObjectOpenHashMap[String] = new Int2ObjectOpenHashMap[String]
    for (elem <- dictFile.collect()) {
      dict.put(elem._2, elem._1)
    }
    val idBroad = sc.sparkContext.broadcast(dict)
    // val KN = getKNSmoothing(grams, ep.N)
    val D = getDArray(grams, ep.N)

    var pre_gram = grams(0).mapPartitions(part => {
      part.map(elem =>{
        (elem._1, Divide(elem._2, count(0)))
      })
    })
      //.union(sc.sparkContext.makeRDD(Array(("<s>", 0.0), ("unk", Divide(1, count(0))))))
      //.partitionBy(new HashPartitioner(ep.partitionNum))
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
              sb.append(0.0).append("\t").append("<s>").append("\t").append(elem._2._2)
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
            .repartition(1).saveAsTextFile(SntToCooc.getKNGramPath(ep.lmRootDir, i))
        } else{
          ///val tuple = spToken(0)

          gram.union(
            sc.sparkContext.makeRDD(
              Array(basic.Divide(1.0, count(i)) + "\tunk\t 0.0", Math.log10(0.5) + "\t</s>\t0.0"))
          ).repartition(ep.partitionNum).saveAsTextFile(SntToCooc.getKNGramPath(ep.lmRootDir, i))
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

        gram.repartition(ep.partitionNum).saveAsTextFile(SntToCooc.getKNGramPath(ep.lmRootDir, i))
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
    }).saveAsTextFile(SntToCooc.getKNGramPath(ep.lmRootDir, ep.N))
  }

  def simpleMap(data : RDD[(String, Int)]) : RDD[(String, (String, Int))] ={
    val prop = data.map(elem => {
      val tmp = elem._1.trim.substring(0, elem._1.trim.lastIndexOf(" "))
      (tmp, (elem._1, elem._2))
    })
    prop
  }

  def getAllCounts(data: RDD[(String, (String, Int))], D : Double): RDD[(String, Double, Double)] = {
    val res = data.groupByKey().map(elem => {
      val arr = elem._2.toArray
      val sum : Long = arr.map(_._2).sum
      val fresh = arr.map(elem => {
        /*val sp = elem._1.trim.split(" ")
        sp(sp.length - 1)*/
        val index = elem._1.indexOf(" ")
        elem._1.trim.substring(index+1)
      }).distinct.length

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

  def Multiply(a : Double, b : Double) : Double = {
    BIGDECIMAL.multiply(a, b)
  }

  def Divide(a : Int, b: Long) : Double ={
    (a * 1.0) / (b * 1.0)
  }

  def SmoothedProbability(count : Long, prefix_count : Long, D : Double) : Double = {
    val max = math.max(count * 1.0 - D , 0)
    var result  = 0.0
    if (max > 0 && prefix_count != 0)
      result = max / (prefix_count * 1.0)
    result
  }

  def getBackOffWeights(total : Long, fresh_num : Int, D : Double) : Double = {
    var result = 0.0
    if (D > 0 && fresh_num > 0)
      result = (D * fresh_num * 1.0) / (total * 1.0)
    result
  }

  def getKNSmoothing(grams :Array[RDD[(String, Int)]], N: Int): Array[KNSmoothing] = {
    val kn = new Array[KNSmoothing](N)
    for (i <- 0 until N) {
      kn(i) = new KNSmoothing(grams(i).map(x => (x._2, 1)).reduceByKey(_+_))
    }
    kn
  }

  def getDArray(grams :Array[RDD[(String, Int)]], N: Int) : Array[Double] = {
    val D = new Array[Double](N)
    for (i <- 0 until N) {
      val kn = new KNSmoothing(grams(i).map(x => (x._2, 1)).reduceByKey(_+_))
      D(i) = kn.getD
    }
    D
  }

  /**
    * this function wants to get a n-grams properties like, prefix_num and the number of unique words that follow the history Wi-n+1 ~ Wi-1 .
    * N1+(Wi-n+1 ~ Wi-1 .) is the number of unique words that follow the words history Wi-n+1 ~ Wi
    *                 N1+(Wi-n+1 ~ Wi-1 .) = |{Wi : C(Wi-n+1 ~ Wi-1 Wi) > 0}|
    *
    *
    * prefix_Str is a word sequence which remove the last of word in word sequence
    * (prefix_Str, (n-gram, count))
    *
    * @param gram n-gram
    * @return (prefix_str, prefix_count, fresh_count)
    */
  def getUniqueWordSequence(gram : RDD[(String, (String, Int))], D : Double) : RDD[(String, Double)] = {
    val num = gram.reduceByKey((a, b) => {
      val sb = new StringBuilder(128)
      sb.append(a._1).append(SpecialString.mainSeparator).append(b._1)
      (sb.toString(), a._2 + b._2)
    }).map(elem => {
      val set = elem._2._1.split(Splitter.mainSplitter).map(x => x.split(" ")).map(x => x(x.length - 1)).distinct
      (elem._1, getBackOffWeights(elem._2._2, set.length, D))
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
      val sb = new StringBuilder
      sb.append(a).append(SpecialString.mainSeparator).append(b)
      sb.toString()
    }).map(elem => {
      val split = elem._2.split(Splitter.mainSplitter).map(x => x.split(" "))
      val set : mutable.HashSet[String] = new mutable.HashSet[String]()
      for (e <- split) {
        set.add(e(0))
      }
      (elem._1, Divide(set.size, count))
    })
    num
  }

  def set(sc: SparkSession, trainData : RDD[String], index: Array[RDD[(String, Long)]], ep: ExtractionParameters): Unit = {
    val PB = new Array[RDD[(Long, Double)]](N)
    val OW = new Array[RDD[(Long, Double)]](N)

    var i = N-1
    var grams = NGram.get_NGram(trainData, N).partitionBy(new HashPartitioner(ep.partitionNum)).cache()
    val KNBroad = new KNSmoothing(grams.map(x => (x._2, 1)).reduceByKey(_+_))
    var D = KNBroad.D.get
    var prop = simpleMap(grams)
    val indexing = index(i)

    /**
      * (key, count, prefix_count) compute prob => (key, prob) join with (key, ID)
      * (ID, prob)
      */

    PB(i) = prop
      .join(prop.map(x => (x._1, x._2._2)).reduceByKey(_+_), ep.partitionNum)
      .map(elem => { (elem._2._1._1, SmoothedProbability(elem._2._1._2, elem._2._2, D)) })
      .join(indexing, ep.partitionNum)
      .map(x => (x._2._2, x._2._1))

    i -= 1

    if (i == 0) {
      val lastIndex = index(i)
      /**
        * (ID, BOW)
        */
      OW(i) = getUniqueWordSequence(prop, D)
        .join(lastIndex, ep.partitionNum)
        .map(x => (x._2._2, x._2._1))

      val count = grams.count()
      PB(i) = getProbabilityOfUnigram(grams.map(x => x._1), count)
        .join(lastIndex, ep.partitionNum)
        .map(x => (x._2._2, x._2._1))

      grams.unpersist()
    }

    while (i > 0) {
      //println("----------------------------------------------- Start round --------------------------------------------------------------")
      val indexing = index(i)

      OW(i) = getUniqueWordSequence(prop, D)
        //.map(x => {(x._1, getBackOffWeights(x._2._1, x._2._2, D))})
        .join(indexing, ep.partitionNum)
        .map(x => (x._2._2, x._2._1))

      grams.unpersist()

      val NextGrams = NGram.get_NGram(trainData, i+1).partitionBy(new HashPartitioner(ep.partitionNum)).cache()

      val KN = new KNSmoothing(NextGrams.map(x => (x._2, 1)).reduceByKey(_+_))
      val prefix_D = KN.D.get
      val NextProp = simpleMap(NextGrams)

      PB(i) = NextProp
        .join(NextProp.map(x => (x._1, x._2._2)).reduceByKey(_+_), ep.partitionNum)
        .map(elem => { (elem._2._1._1, SmoothedProbability(elem._2._1._2, elem._2._2, prefix_D)) })
        .join(indexing, ep.partitionNum)
        .map(x => (x._2._2, x._2._1))


      if (i == 1){
        val lastIndex = index(i-1)

        OW(i-1) = getUniqueWordSequence(NextProp, prefix_D)
          //.map(x => {(x._1, getBackOffWeights(x._2._1, x._2._2, prefix_D))})
          .join(lastIndex, ep.partitionNum)
          .map(x => (x._2._2, x._2._1))

        val count = NextGrams.count()
        PB(i-1) = getProbabilityOfUnigram(NextGrams.map(x => x._1), count)
          .join(lastIndex, ep.partitionNum)
          .map(x => (x._2._2, x._2._1))

        NextGrams.unpersist()
      } else {
        grams = NextGrams
        D = prefix_D
        prop = NextProp
      }
      //println("----------------------------------------------- Done  round --------------------------------------------------------------")
      i -= 1
    }
    prob = Some(PB)
    BOW = Some(OW)
  }

  /**
    * this function is initialize bloomFilter
    *
    * @param probA is the ID for n-grams
    * @param bow is the backoff
    * @param p is the value the expected error rate for BloomFilter
    */
  def setBloomFilter(probA : Array[RDD[(Long, Double)]], bow : Array[RDD[(Long, Double)]],  p : Double): Unit = {

    val filter : Array[BloomFilter[Long]] = new Array[BloomFilter[Long]](N)
    for (i <- 0 until N) {
      var setA : Option[Array[Long]] = None
      if (i == (N - 1)) {
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
    //bloomFilter = Some(filter)
  }
}
