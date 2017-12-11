package edu.nju.pasalab.mt.wordAlignment.prodcls

import java.io.IOException
import edu.nju.pasalab.mt.extraction.dataStructure.{Splitter, SpecialString}
import edu.nju.pasalab.mt.util.CommonFileOperations
import gnu.trove.map.hash.TIntLongHashMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.slf4j.{LoggerFactory, Logger}

import scala.util.Random

/**
  * Created by YWJ on 2016/6/7.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object UpdateClass {
  val logger:Logger  = LoggerFactory.getLogger(UpdateClass.getClass)

  val expected = 300
  val factor =  .8f
  def updateClass(bigram : RDD[String], clsBigram : RDD[(Long, Long)] , output : String, cls : Int,
                  sc : SparkSession): RDD[String] = {

//    val mapNVC : Array[TIntLongHashMap] = new Array[TIntLongHashMap](cls + 1)
//    val arNC : Array[Long] = new Array[Long](cls + 1)
//    for (i <- 0 until cls + 1) {
//      mapNVC(i) = new TIntLongHashMap(expected, factor)
//    }
//    for (elem <- clsBigram.collect()) {
//      val vc = elem._1
//      val c = getClassID(vc, cls).toInt
//      mapNVC(c).put(getV(vc, cls).toInt, elem._2)
//      arNC(c) += elem._2
//    }
//
//    val mapNVC_broadcast = sc.broadcast(mapNVC)
//    val arNC_broadcast = sc.broadcast(arNC)

    val clsBigram_brodcast = sc.sparkContext.broadcast(clsBigram.collect())
    //(key1 || key2 || classID || count)
    val gram = bigram.map(line => {
      val split = line.trim.split("\\s+")
      (split(1).toInt, String.format("%s %s %s", split(0), split(2), split(3)))
    }).reduceByKey((a, b) => {
      a + SpecialString.mainSeparator + b
    }).mapPartitions(part => {

      val cls_Bigram = clsBigram_brodcast.value

      val mapNVC : Array[TIntLongHashMap] = new Array[TIntLongHashMap](cls + 1)
      val arNC : Array[Long] = new Array[Long](cls + 1)
      for (i <- 0 until cls + 1) {
        mapNVC(i) = new TIntLongHashMap(expected, factor)
      }
      for (elem <- cls_Bigram) {
        val vc = elem._1
        val c = getClassID(vc, cls).toInt
        mapNVC(c).put(getV(vc, cls).toInt, elem._2)
        arNC(c) += elem._2
      }

      //val NVC = mapNVC_broadcast.value
      //val NC = arNC_broadcast.value

      part.map(elem => initFunction(elem, mapNVC, arNC))
    })

    logger.info("<1>. Class Map save to " + output)
    System.out.println("<1>. Class Map save to " + output)
    CommonFileOperations.deleteIfExists(output)
    gram.saveAsTextFile(output)
    gram
  }

  def initFunction(tuple : (Int, String), mapNVC : Array[TIntLongHashMap], arNC : Array[Long]): String = {
    val value = tuple._2.trim.split(Splitter.mainSplitter)
    val mapNVW = new TIntLongHashMap()
    var cw : Int = -1
    var nw : Long = 0L

    for (item <- value) {
      val cont = item.trim.split("\\s+")
      if (cont.length != 3)
        logger.info("BAD N(v, w), entry", throw new IOException("BAD N(v, w), entry"))
      val cw1 = cont(1).toInt
      if (cw == -1)
        cw = cw1
      else if (cw1 != cw)
        throw new IOException("Inconsistent original class " + cw1 + " " + cw)

      mapNVW.put(cont(0).toInt, cont(2).toLong)
      nw += cont(2).toLong
    }

    val  newCW = determineBestClass(mapNVC, mapNVW, nw, arNC, cw)

    if (newCW != cw) {
      //need to update
      arNC(newCW) += nw
      arNC(cw) -= nw
      val it = mapNVW.iterator()
      while (it.hasNext) {
        it.advance()
        var origin = mapNVC(newCW).get(it.key())
        var newValue = origin + it.value()
        mapNVC(newCW).put(it.key(), newValue)

        origin = mapNVC(cw).get(it.key())
        newValue = origin - it.value()
        if (newValue <= 0)
          mapNVC(cw).remove(it.key())
        else
          mapNVC(cw).put(it.key(), newValue)
      }
    }
//    if (newCW > 51) {
//      val randomNum=(new Random).nextInt(51)
//    }
    //String.format("%s %s %s", tuple._1, newCW.toString, nw.toString)
    val part2 = if (newCW == 50) (new Random).nextInt(50) else newCW

    tuple._1 + " " + part2 + " " + nw
  }

  def determineBestClass(mapNVC : Array[TIntLongHashMap], mapNVW: TIntLongHashMap,
                         nw: Long, arNC : Array[Long], cw : Int) : Int = {
    var bestDLL : Double = 0
    var newCW = cw
    var deltaRemove : Double = arNC(cw) * Math.log(arNC(cw))
    var ncmnw = arNC(cw) - nw
    if (ncmnw <= 0)
      return cw

    deltaRemove -= ncmnw * Math.log(ncmnw)
    val it = mapNVW.iterator()
    while(it.hasNext) {
      it.advance()
      val nvwc = mapNVC(cw).get(it.key())
      deltaRemove -= nvwc * Math.log(nvwc)

      var nvcmnvw = (nvwc - it.value()) * 1.0
      val x = nvcmnvw match {
        case 0 => 0
        case _ => Math.log(nvcmnvw)
      }
     // val A = if (nvcmnvw == 0) 0 else Math.log(nvcmnvw)
      nvcmnvw *= x
      deltaRemove += nvcmnvw
    }

    for (c <- 1 until arNC.length ) {
      if (c != cw) {
        var deltaMove : Double = arNC(c) * Math.log(arNC(c))
        ncmnw = arNC(c) + nw
        deltaMove -= ncmnw * Math.log(ncmnw)

        val it1 = mapNVW.iterator()
        while(it1.hasNext) {
          it1.advance()
          val nvwc = mapNVC(c).get(it1.key())
          val x = nvwc match {
            case 0 => 0
            case _ => Math.log(nvwc)
          }
          //val A = if(nvwc == 0) 0 else nvwc * Math.log(nvwc)
          deltaMove -= x

          var nvcmnvw: Double = nvwc + it1.value()

          nvcmnvw *= Math.log(nvcmnvw)
          deltaMove += nvcmnvw
        }
        val delta: Double = deltaRemove + deltaMove
        if (delta > bestDLL) {
          bestDLL = delta
          newCW = c
        }
      }
    }
    newCW
  }
  def getClassID(key : Long, cls : Int): Long = {
    key % (cls + 1)
  }
  def getV(key : Long, cls : Int) : Long = {
    key / (cls + 1)
  }
}
