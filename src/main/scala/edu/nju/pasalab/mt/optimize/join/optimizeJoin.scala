package edu.nju.pasalab.mt.optimize.join

import com.twitter.algebird.CMSHasher
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.language.implicitConversions

/**
  * Created by YWJ on 2016.12.29.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
trait optimizeJoin {
  implicit def rddToSkewJoinOperations_e94qoy3tnt[K: ClassTag: Ordering: CMSHasher, V: ClassTag](rdd: RDD[(K, V)]): leanJoinOperation[K, V] =
    new leanJoinOperation(rdd)

  implicit def rddToBlockJoinOperations_7IaIe6dkih[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): blockJoinOperation[K, V] =
    new blockJoinOperation(rdd)

  implicit def rddToBroadCastJoinOperations_xe7xnsja3n[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) : broadcastJoin[K, V] =
    new broadcastJoin(rdd)
}
object optimizeJoin extends optimizeJoin
