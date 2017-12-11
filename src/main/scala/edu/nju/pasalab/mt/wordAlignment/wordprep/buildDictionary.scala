package edu.nju.pasalab.mt.wordAlignment.wordprep

import edu.nju.pasalab.mt.util.CommonFileOperations
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable.ArrayBuffer

/**
  * srcDicTemp means not sorted file, here we did not save temp file
  * Created by YWJ on 2016/6/11.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object buildDictionary {
  val logger:Logger  = LoggerFactory.getLogger(buildDictionary.getClass)

  def execute(mergedCorpus : RDD[Array[String]],  sourceDict : String, targetDict : String,
              sc :SparkSession): Unit = {
    //val result : Array[RDD[String]] = new Array[RDD[String]](2)
    val srcTemp = mergedCorpus.flatMap(line => line(0).trim.split("\\s+").map(x => (x, 1)))
      .reduceByKey(_+_)
 //     .map(x => (x._2, x._1)).repartition(1)
//      .sortByKey(false)
//      .zipWithIndex()
//      .map(elem => { (elem._2 + 2) + "\t" + elem._1._2 + "\t" + elem._1._1 })


    logger.info("\t<3>. Source Dictionary save as to " + sourceDict + " type: (ID, Word, freq)")
    println("\t<3>. Source Dictionary save as to " + sourceDict + " type : (ID, Word, freq)")

    CommonFileOperations.deleteIfExists(sourceDict)

    val srcArr : ArrayBuffer[String] = new ArrayBuffer[String]
    srcArr.append("1\tUNK\t0")
    //src_pr.println("1\tUNK\t0")
    var count = 2
    val src = srcTemp.collect().toList.filter(_._1.length > 0).sortWith(_._2 > _._2)
    for (elem <- src) {
      srcArr.append(count + "\t" + elem._1 + "\t" + elem._2)
      //src_pr.println(count + "\t" + elem._1 + "\t" + elem._2)
      count += 1
    }
    sc.sparkContext.makeRDD(srcArr.toArray[String], 1).saveAsTextFile(sourceDict)
    //src_pr.close()


    val tgtTemp = mergedCorpus.flatMap(line => line(1).trim.split("\\s+").map(x => (x, 1)))
      .reduceByKey( _+_)
//      .map(x => (x._2, x._1))
//      .sortByKey(false)
//      .zipWithIndex()
//      .map(elem => { (elem._2 + 2) + "\t" + elem._1._2 + "\t" + elem._1._1 })

    logger.info("\t<3>. target Dictionary save as to " + targetDict + " type: (ID, Word, freq)")
    println("\t<3>. target Dictionary save as to " + targetDict + " type : (ID, Word, freq)")

    CommonFileOperations.deleteIfExists(targetDict)
    val tgtArr : ArrayBuffer[String] = new ArrayBuffer[String]
    tgtArr.append("1\tUNK\t0")

    //tgt_pr.println("1\tUNK\t0")
    var count2 = 2
    val tgt = tgtTemp.collect().toList.filter(_._1.length > 0).sortWith(_._2 > _._2)
    for (elem <- tgt) {
      tgtArr.append(count + "\t" + elem._1 + "\t" + elem._2)
      //tgt_pr.println(count + "\t" + elem._1 + "\t" + elem._2)
      count2 += 1
    }
    sc.sparkContext.makeRDD(tgtArr.toArray[String], 1).saveAsTextFile(targetDict)
    //tgt_pr.close()


    //logInfo("srcTemp RDD partitions number : " + srcTemp.partitions.size)
    //println("srcTemp RDD partitions number : " + srcTemp.partitions.size)
    //val tgt = sc.makeRDD(Array("1\tUNK\t0")).union(tgtTemp)
    //tgt.repartition(1).saveAsTextFile(targetDict)
    //result(0) = src
    //result(1) = tgt
    //result

    //    val src_pr: PrintWriter = new PrintWriter(new BufferedWriter
    //    (new OutputStreamWriter(CommonFileOperations.openFileForWrite(sourceDict, true)), 65 * 1024 * 1024))
  }
}
