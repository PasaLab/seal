package edu.nju.pasalab.mt.wordAlignment.prodcls

import edu.nju.pasalab.mt.util.CommonFileOperations
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by YWJ on 2016/6/7.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object Likelihood {

  val logger:Logger  = LoggerFactory.getLogger(Likelihood.getClass)
  def getLikelihood(bigram : RDD[(Long, Long)], mapClass : RDD[String], output : String, workingDir : String, sc : SparkSession): RDD[Double] = {
    val temp1 = workingDir + "/part1"

    //(1, double)
    val NVC = bigram.map(elem => (1, elem._2 * math.log(elem._2 * 1.0))).reduceByKey(_+_)

    logger.info("\tLikelihood part1 save to " + temp1)
    System.err.println("\tLikelihood part1 save to " + temp1)
    CommonFileOperations.deleteIfExists(temp1)
    //NVC.map(x => x._1 + " " + x._2).saveAsTextFile(temp1)

    val temp2 = workingDir + "/part2"
    val NWC = mapClass.map(line => {
      val sp = line.trim.split("\\s+")
      (sp(1).toInt, sp(2).toLong)
    }).groupByKey().map(elem =>{
      val arr = elem._2.toArray
      var count : Long = 0
      for (e <- arr)
        count += e
      var value : Double = 0
      val M = - Math.log(count * 1.0)
      for (e <- arr)
        value += e * M
      (elem._1, value)
    })

    logger.info("<2>. Likelihood part2 save to " + temp2)
    System.err.println("<2>. Likelihood part2 save to " + temp2)
    CommonFileOperations.deleteIfExists(temp2)
    //NWC.map(x => x._1 + " " + x._2).saveAsTextFile(temp2)

    val out = NVC.union(NWC).map(x => x._2)

    logger.info("\tLikelihood final results save to " + output)
    System.err.println("\tLikelihood final results save to " + output)
    CommonFileOperations.deleteIfExists(output)
    //out.saveAsTextFile(output)
    out
  }
}
