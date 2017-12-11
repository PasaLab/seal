package edu.nju.pasalab.mt.optimize.join

import java.util.{Random => JRandom}

import org.apache.spark.Partitioner
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by YWJ on 2016.12.18.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
class blockJoinOperation[K: ClassTag, V: ClassTag](rdd : RDD[(K, V)]) extends Serializable {

  private def blockCoGroup[W](ot : RDD[(K, W)], leftReplication : Int, rightReplication : Int, partitioner : Partitioner)
  :RDD[((K, (Int, Int)), (Iterable[V], Iterable[W]))] = {
    assert(leftReplication >= 1, "must specify a positive number for left replication")
    assert(rightReplication >= 1, "must specify a positive number for right replication")

    def getReplication(random : JRandom, replication : Int, otherReplication : Int) : Seq[(Int, Int)] = {
      val randomNum = random.nextInt(otherReplication)
      (0  until replication).map(elem => (randomNum, elem))
    }
    val RDDBlocked = rdd.mapPartitions(part => {
      val rand = new JRandom
      part.flatMap(elem => {
        getReplication(rand, leftReplication, rightReplication).map{ rl => ((elem._1, rl.swap), elem._2)}
      })
    })
    val otherBlocked = ot.mapPartitions(part => {
      val rand = new JRandom
      part.flatMap(elem => {
        getReplication(rand, rightReplication, leftReplication).map{ lr => ((elem._1, lr), elem._2)}
      })
    })
    RDDBlocked.cogroup(otherBlocked, partitioner)
  }
  /**
    * Same as join, but uses a block join, otherwise known as a replicate fragment join.
    * This is useful in cases where the data has extreme skew.
    * The input params leftReplication and rightReplication control the replication of the left
    * (this rdd) and right (other rdd) respectively.
    */
  def blockJoin[W](ot : RDD[(K, W)], leftReplication : Int, rightReplication : Int, partitioner : Partitioner) : RDD[(K, (V, W))] = {
    blockCoGroup(ot, leftReplication, rightReplication, partitioner).flatMap(pair => {
      for (part1 <- pair._2._1.iterator; part2 <- pair._2._2.iterator) yield (pair._1._1, (part1, part2))
    })
  }

  def blockJoin[W](ot : RDD[(K, W)], leftReplication : Int, rightReplication : Int) : RDD[(K, (V, W))] ={
    blockJoin(ot, leftReplication, rightReplication, defaultPartitioner(rdd, ot))
  }

  /**
    * Same as leftOuterJoin, but uses a block join, otherwise known as a replicate fragment join.
    * This is useful in cases where the data has extreme skew.
    * The input param rightReplication controls the replication of the right (other rdd).
    */
  def blockLeftOutJoin[W](ot: RDD[(K, W)], rightReplication: Int, partitioner: Partitioner): RDD[(K, (V, Option[W]))] = {
    blockCoGroup(ot, 1, rightReplication, partitioner).flatMap({
      case ((k, _), (itv, Seq())) => itv.iterator.map(elem => (k, (elem, None)))
      case ((k, _), (itv, itw)) => for (part1 <- itv; part2 <- itw) yield (k , (part1, Some(part2)))
    })
  }

  def blockLeftOutJoin[W](ot: RDD[(K, W)], rightReplication: Int): RDD[(K, (V, Option[W]))] = {
    blockLeftOutJoin(ot, rightReplication, defaultPartitioner(rdd, ot))
  }


  /**
    * Same as rightOuterJoin, but uses a block join, otherwise known as a replicate fragment join.
    * This is useful in cases where the data has extreme skew.
    * The input param leftReplication controls the replication of the left (this rdd).
    */
  def  blockRightOutJoin[W](ot: RDD[(K, W)], leftReplication: Int, partitioner: Partitioner): RDD[(K, (Option[V], W))] = {
    blockCoGroup(ot, leftReplication, 1, partitioner).flatMap {
      case ((k, _), (Seq(), itw)) => itw.iterator.map(part2 => (k, (None, part2)))
      case ((k, _), (itv, itw)) => for (part1 <- itv; part2 <- itw) yield (k, (Some(part1), part2))
    }
  }

  def blockRightOutJoin[W](ot: RDD[(K, W)], leftReplication: Int): RDD[(K, (Option[V], W))] = {
    blockRightOutJoin(ot, leftReplication, defaultPartitioner(rdd, ot))
  }


}
