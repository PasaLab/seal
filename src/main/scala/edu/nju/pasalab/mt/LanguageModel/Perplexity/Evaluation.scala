package edu.nju.pasalab.mt.LanguageModel.Perplexity

import edu.nju.pasalab.mt.LanguageModel.Smoothing.{GTSmoothing, KNSmoothing, MKNSmoothing}
import edu.nju.pasalab.mt.LanguageModel.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer
/**
  * Created by wuyan on 2015/12/23.
  */
@deprecated
object Evaluation {

  /**
    * @param str : represent the sentence
    * @param n   : represent the value of the n-gram. such 3-gram , n = 3
    * @return    : return the Sentence N-gram Array
    */
  @deprecated
  def getSentence(str : String, n : Int) : Array[String] = {
    val split = str.split(" ")
    val arr : ArrayBuffer[String] = new ArrayBuffer[String]()
    /**
      * First We Should get the word Sequence
      * For a sentence S composed of the words "w1w2w3..wl", we can express P(S) as :
      *  P(S) = P(w1|<BOS>) * P(w2|<BOS>w1) * ... * P(<EOS>|<BOS>w1...wl)
      *  we make the approximation that the probability of a word only depend on the identity of
      *  the immediately preceding n-1 words, give us :
      *
      *  for (i <- 1 until l + 1)
      *    P(S) *= P(Wi|Wi-n+1 ~ Wi-1)
      *
      * Note: <BOS> is the beginning of the Sentence and the <EOS> is the end of the Sentence
      */
    var i  = 1
    while (i < split.length ) {
      val builder = new StringBuilder(32)
      var begin = i - n + 1
      val end = i - 1
      while (begin < 0) begin += 1
      builder.append(split(begin))
      for (index <- begin+1 until end + 1) {
        builder.append(" ").append(split(index))
      }
      builder.append(" ").append(split(i))
      arr.append(builder.toString())
      i += 1
    }
    arr.toArray
  }

  /**
    * @param trainData
    * @param testSet
    * @param n
    * @param sc
    * @return
    */
  @deprecated
  def computeMKNPerplexity(trainData : RDD[String], testSet: Array[String], n:Int, sc : SparkSession): Double = {
    // this is used to implicitly convert an RDD to a DataFrame.
    import sc.sqlContext.implicits._

    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).distinct.length * 1.0
    val array = testSet.map(x => getSentence(x, n))
    val prob = array.map(x => (new Array[Double](x.length)).map(x => 0.0))

    var N = 2
    //    import java.io._
    //    val ps1 = new PrintStream("F:/workspace/MKN_log.txt")

    while (N <= n) {
      val gram = NGram.get_NGram(trainData, N)
      val MKN : MKNSmoothing = new MKNSmoothing(gram.map(x => (x._2, 1)).reduceByKey(_+_))
      val table = gram.map(x => N_GRAM(x._1, x._2)).toDF().cache()
      val lower = MKN.getLowOrderNum(gram).map(x => PREFIX(x._1 , x._2)).toDF().cache()
      //      var uni_table : Option[DataFrame] = None
      //      if (N == 2) {
      //        val count = gram.map(x => x._2).reduce(_+_) * 1.0
      //        uni_table = Some(MKN.getProbabilityOfUnigram(gram, count).map(x => UNI_GRAM(x._1,x._2)).toDF())
      //      }

      for (i <- 0 until array.length) {
        val Sentence = array(i)

        for (index <- 0 until Sentence.length) {
          //process a sentence
          val sb = new StringBuilder(64)
          var prefix_Str : Option[String] = None
          val split = Sentence(index).split(" ")
          val len = split.length - N
          if (len >= 0) {
            sb.append(split(len))
            for (j <- len+1 until split.length) {
              if (j == split.length - 1) {
                prefix_Str = Some(sb.toString())
              }
              sb.append(" ").append(split(j))
            }

            val numStr_ = table.filter(table("words") === sb.toString()).collect()
            var numStr : Option[Long] = None
            if (numStr_.length > 0) {
              numStr = Some(numStr_(0)(1).toString().toLong)
            } else {
              numStr = Some(0L)
            }
            val lowerOrder = lower.filter(lower("words") === prefix_Str.get).collect()
            var lowerNum : Array[Long] = new Array[Long](4).map(x => 0L)
            if (lowerOrder.length > 0)
              lowerNum = lowerOrder(0)(1).toString.split("\t").map(_.toLong)

            val part1 = MKN.getSmoothedCountPart1(numStr.get, lowerNum(0))
            if (N == 2) {
              //val uni = uni_table.get.filter(uni_table.get("words") === split(split.length - 1)).collect()
              //val prefix = if(uni.length > 0) uni(0)(1).toString.toDouble else 0.0
              prob(i)(index) = part1 + MKN.getBackOffWeight(lowerNum(0), lowerNum(1), lowerNum(2), lowerNum(3))
              //ps1.println("part 1: " + part1 + " BOW: " + MKN.getBackOffWeight(lowerNum(0), lowerNum(1), lowerNum(2), lowerNum(3)) + " prob(i)(index): "  + prob(i)(index))
            }else {
              val prefix_prob = prob(i)(index)
              prob(i)(index) = part1 + MKN.getBackOffWeight(lowerNum(0), lowerNum(1), lowerNum(2), lowerNum(3)) * prefix_prob

              // ps1.println("part 1: " + part1 + " BOW: " + MKN.getBackOffWeight(lowerNum(0), lowerNum(1), lowerNum(2), lowerNum(3)) + " prob(i)(index): " + prob(i)(index))
            }
          } // end if
        } // end for
      } // end for
      N += 1
      table.unpersist()
      lower.unpersist()
    } // end while
    val arr = prob.map(x => x.reduce(_*_))
    var probability = 1.0
    println(num + "   " + 1.0/num)
    for (elem <- arr) {
      if (elem != 0.0)
        probability *= Math.pow((1.0/elem) , (1.0/num))
      println(elem + " |||  " + probability)
    }
    //ps1.close()
    probability
  }
  /**
    *
    * @param trainData : data is the train Set of the Language Model
    * @param testSet   : testSet is the test Set of the Language Model
    * @param n         : n is the value of n-gram
    * @param sqlContext: sqlContext is the SparkSQL environment
    * @return          : return the Perplexity of the LM on testSet
    */
  def computeKNPerplexity(trainData : RDD[String] , testSet : Array[String], n: Int, sqlContext : SQLContext) : Double = {
    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).distinct.length * 1.0

    val array = testSet.map(x => getSentence(x, n))
    val prob = KN_Recursive(trainData, array, n, sqlContext, num)

    var probability = 1.0
    for (elem <- prob) {
      if (elem != 0.0)
        probability *= Math.pow((1.0/elem) , (1.0/num))
    }
    probability
  }

  /**
    *
    * @param data : data is the train Set of the Language Model
    * @param test   : testSet is the test Set of the Language Model
    * @param n         : n is the value of n-gram
    * @param sqlContext : sqlContext is the SparkSQL environment
    * @param num : the total number
    * @return  : return the probability of every sentence
    */
  @deprecated
  def KN_Recursive(data : RDD[String] , test : Array[Array[String]], n: Int,
                   sqlContext : SQLContext, num : Double) : Array[Double] = {
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val prob = test.map(x => (new Array[Double](x.length)).map(x => 0.0))
    var N = 2
    while (N <= n) {
      val gram = NGram.get_NGram(data, N)
      val KN : KNSmoothing = new KNSmoothing(gram.map(x => (x._2, 1)).reduceByKey(_+_))
      val table = gram.map(x => N_GRAM(x._1, x._2)).toDF().persist()
      val fresh_pair = KN.getUniqueNum(gram).map(x => PREFIX(x._1, x._2)).toDF().persist()

      var uni_table : Option[DataFrame] = None
      if (N == 2) {
        val count = gram.count() * 1.0
        uni_table = Some(KN.getProbabilityOfUnigram(gram, count).map(x => UNI_GRAM(x._1,x._2)).toDF())
      }

      for (i <- 0 until test.length) {
        val Sentence = test(i)

        for (index <- 0 until Sentence.length) {
          //process a sentence
          val sb = new StringBuilder(32)
          var prefix_Str : Option[String] = None
          val split = Sentence(index).split(" ")
          val len = split.length - N
          if (len >= 0) {
            sb.append(split(len))
            for (j <- len+1 until split.length) {
              if (j == split.length - 1) {
                prefix_Str = Some(sb.toString())
              }
              sb.append(" ").append(split(j))
            }

            val numStr_ = table.filter(table("words") === sb.toString()).collect()
            var numStr : Option[Int] = None
            if (numStr_.length > 0) {
              numStr = Some(numStr_(0)(1).toString().toInt)
            } else {
              numStr = Some(0)
            }
            val filter_str = fresh_pair.filter(fresh_pair("words") === prefix_Str.get).collect()
            var filter : Array[Int] = new Array[Long](2).map(x => 0)
            if (filter_str.length > 0) {
              filter = filter_str(0)(1).toString.split("\t").map(_.toInt)
            }

            val num = KN.getSmoothedCountPart1(numStr.get, filter(1))

            if (filter(0) == 0 || filter(1) == 0) {
              prob(i)(index) = num
            } else {
              if (N == 2) {
                val uni = uni_table.get.filter(uni_table.get("words") === split(split.length - 1)).collect()
                val prefix = if(uni.length > 0) uni(0)(1).toString.toDouble else 0.0
                prob(i)(index) = num + KN.getBackOffWeights(filter(0) , filter(1)) * prefix
              } else {
                val prefix_prob = prob(i)(index)
                prob(i)(index) = num + KN.getBackOffWeights(filter(0), filter(1)) * prefix_prob
              }
            }
          } // end if
        } // end for
      } // end for
      N += 1
      fresh_pair.unpersist()
      table.unpersist()
    } // end while

    val s = prob.map(x => x.reduce(_*_))
    s
  }

  /**
    * this function is aimed at get the Evaluation Perplexity of  the Language Model on the testSet
    * For a testSet composed of the sentences(T1,...Tn),we can calculate the Probability of the testSet :
    *      for (i <- 0 until n )
    *         P(T) *= p(Ti)
    *
    * @param trainData : data is the train Set of the Language Model
    * @param testSet   : testSet is the test Set of the Language Model
    * @param n         : n is the value of n-gram
    * @param sqlContext : sqlContext is the SparkSQL environment
    * @return  : return the Perplexity of the LM on testSet
    */
  @deprecated
  def computePerplexity(trainData : RDD[String] , testSet : Array[String], n: Int, sqlContext : SQLContext) : Double = {
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    //val num = testSet.length * 1.0
    val num = testSet.map(x => x.split(" ")).flatMap(x => x).filter(x => !x.equals("<s>")).distinct.length * 1.0
    val testArray = testSet.map(x => getSentence(x, n))

    val start = n - 2
    val arr : Array[Double] = preProcessing(trainData, start, testArray, n, sqlContext)

    val prefix_gram = NGram.get_NGram(trainData, (n-1))
    val prefix_GTS = new GTSmoothing(prefix_gram.map(x => (x._2, 1)).reduceByKey(_+_))
    val prefix_table = prefix_gram.map(elem => (elem._1, prefix_GTS.getSmoothedCount(elem._2))).map(x => N_GRAM1(x._1, x._2)).toDF().cache()

    val gram = NGram.get_NGram(trainData, n)
    val GTS = new GTSmoothing(gram.map(x => (x._2, 1)).reduceByKey(_+_))
    val table = gram.map(elem => (elem._1, prefix_GTS.getSmoothedCount(elem._2))).map(x => N_GRAM1(x._1, x._2)).toDF().cache()

    for (i <- 0 until testArray.length) {
      if (testArray(i).length >= start)
        for (index <- start until testArray(i).length){
          val prefix_str = testArray(i)(index).substring(0,testArray(i)(index).lastIndexOf(" "))
          val prefix = prefix_table.filter(prefix_table("words") === prefix_str).collect()
          var prefix_num : Option[Double] = None
          if (prefix.length > 0) {
            prefix_num = Some(prefix(0)(1).toString.toDouble)
          } else {
            prefix_num = Some(prefix_GTS.getSmoothedCount(0L))
          }

          val numStr = table.filter(table("words") === testArray(i)(index)).collect()
          var num : Option[Double] = None
          if (numStr.length > 0) {
            num = Some(numStr(0)(1).toString.toDouble)
          } else {
            num = Some(GTS.getSmoothedCount(0L))
          }

          arr(i) *= num.get / prefix_num.get
        } // end for
    }
    table.unpersist()
    prefix_table.unpersist()
    var prob = 1.0
    for (elem <- arr) {
      prob *= math.pow((1.0/elem) , (1.0/num))
    }
    prob
  }

  @deprecated
  def preProcessing(data : RDD[String] , start : Int, test : Array[Array[String]], n : Int, sqlContext : SQLContext): Array[Double] = {
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val arr : Array[Double] = (new Array[Double](test.length)).map(x => 1.0)

    var N = 2
    var prefix_gram = NGram.get_NGram(data, (N-1))
    var prefix_GTS = new GTSmoothing(prefix_gram.map(x => (x._2, 1)).reduceByKey(_+_))
    var prefix_table = prefix_gram.map(elem => (elem._1, prefix_GTS.getSmoothedCount(elem._2))).map(x => N_GRAM1(x._1, x._2)).toDF()
    //prefix_gram.map(x => N_GRAM(x._1, x._2)).toDF()

    var gram = NGram.get_NGram(data, N)
    var GTS = new GTSmoothing(gram.map(x => (x._2, 1)).reduceByKey(_+_))
    var table = gram.map(elem => (elem._1, prefix_GTS.getSmoothedCount(elem._2))).map(x => N_GRAM1(x._1, x._2)).toDF()
    //gram.map(x => N_GRAM(x._1, x._2)).toDF()

    if (start > 0) {
      var index = 0
      while (index < start){
        for (i <- 0 until test.length) {
          if (test(i).length > index) {
            val prefix_str = test(i)(index).substring(0, test(i)(index).lastIndexOf(" "))
            val prefix = prefix_table.filter(prefix_table("words") === prefix_str).collect()
            var prefix_num : Option[Double] = None
            if (prefix.length > 0) {
              prefix_num = Some(prefix(0)(1).toString.toDouble)
            } else {
              prefix_num = Some(prefix_GTS.getSmoothedCount(0L))
            }

            val numStr = table.filter(table("words") === test(i)(index)).collect()
            var num : Option[Double] = None
            if (numStr.length > 0) {
              num = Some(numStr(0)(1).toString.toDouble)
            } else {
              num = Some(GTS.getSmoothedCount(0L))
            }
            arr(i) *= num.get / prefix_num.get
          }
        }
        index += 1
        N += 1
        if (N < n) {
          prefix_gram = gram
          prefix_GTS = GTS
          prefix_table = table

          gram = NGram.get_NGram(data, N)
          GTS =  new GTSmoothing(gram.map(x => (x._2, 1)).reduceByKey(_+_))
          table = gram.map(elem => (elem._1, prefix_GTS.getSmoothedCount(elem._2))).map(x => N_GRAM1(x._1, x._2)).toDF()
          //gram.map(x => N_GRAM(x._1, x._2)).toDF()
        }// end if
      } // end while
    }// end if
    arr
  }

}
