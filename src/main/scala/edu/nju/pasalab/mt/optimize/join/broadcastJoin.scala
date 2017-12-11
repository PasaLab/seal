package edu.nju.pasalab.mt.optimize.join

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by YWJ on 2016.12.28.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  *
  * map-side-join-in-spark
  * Join of two or more data sets is one of the most widely used operations you do with your data,
  * but in distributed systems it can be a huge headache. In general, since your data are distributed among many nodes,
  * they have to be shuffled before a join that causes significant network I/O and slow performance.
  *
  * Fortunately, if you need to join a large table (fact) with relatively small tables (dimensions) i.e.
  * to perform a star-schema join you can avoid sending all data of the large table over the network.
  * This type of join is called map-side join in Hadoop community. In other distributed systems,
  * it is often called replicated or broadcast join.
  */
class broadcastJoin[K : ClassTag, V : ClassTag] (rdd : RDD[(K, V)]) extends Serializable {

  def broadCastJoin[W](other : RDD[(K, W)]) : RDD[(K, (V, W))] = {
    val map = other.sparkContext.broadcast(other.collect().toMap)
    rdd.mapPartitions(part => {
      val otMap = map.value
      part.map(elem => {
        (elem._1, (elem._2, otMap.get(elem._1).get))
      })
    })
  }

  def normalJoin[W](other : RDD[(K, W)]) : RDD[(K, (V, W))] = {
    rdd.join(other)
  }
}
