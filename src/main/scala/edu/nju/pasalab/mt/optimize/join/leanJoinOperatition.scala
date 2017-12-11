package edu.nju.pasalab.mt.optimize.join

import java.util.{ Random => JRandom }
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.util.sketch
import com.twitter.algebird.{ CMS, CMSHasher, CMSMonoid }

/**
  * Created by YWJ on 2016.12.18.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  * This join use Count-Mini-Sketch algorithm, see https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch
  */

case class CMSPara(ep : Double = 0.005, delta : Double = 1e-8, seed : Int = 1) {
  def getCMSMonoid[K : Ordering : CMSHasher] : CMSMonoid[K] = CMS.monoid(ep, delta, seed)
}

class leanJoinOperation[K : ClassTag : Ordering : CMSHasher, V : ClassTag](rdd : RDD[(K, V)]) extends Serializable {
  private def getReplicationFactors(rand : JRandom, replication : Int, otherRep : Int) : Seq[(Int, Int)] = {
    require(replication > 0 && otherRep > 0, "replication must be positive")
    val randNum = rand.nextInt(otherRep)
    (0 until replication).map(elem => (randNum, elem))
  }

  private def createRDDCMS[K](rdd : RDD[K], cmsMonoid : CMSMonoid[K]) : CMS[K] =

    rdd.map(elem => cmsMonoid.create(elem)).reduce(cmsMonoid.plus(_, _))


  def leanCoGroup[W: ClassTag](ot : RDD[(K, W)], partitioner: Partitioner,
                              leanReplication : leanReplication = defaultLeanReplication(), cmsPara : CMSPara = CMSPara())
  : RDD[(K, (Iterable[V], Iterable[W]))] = {

    val numPartitions = partitioner.numPartitions
    val broadCastLeftCMS = rdd.sparkContext.broadcast(createRDDCMS[K](rdd.keys, cmsPara.getCMSMonoid[K]))
    val broadCastRightCMS = rdd.sparkContext.broadcast(createRDDCMS[K](ot.keys, cmsPara.getCMSMonoid[K]))

    val rddSkewed = rdd.mapPartitions{ it =>
      val random = new JRandom
      it.flatMap{ elem =>
        val (leftReplication, rightReplication) = leanReplication.getReplication(
          broadCastLeftCMS.value.frequency(elem._1).estimate,
          broadCastRightCMS.value.frequency(elem._1).estimate,
          numPartitions)
        getReplicationFactors(random, leftReplication, rightReplication).map(rl =>((elem._1, rl.swap), elem._2))
      }
    }

    val otherSkewed = ot.mapPartitions{ part =>
      val random = new JRandom
      part.flatMap{ elem =>
        val (leftReplication, rightReplication) = leanReplication.getReplication(
          broadCastLeftCMS.value.frequency(elem._1).estimate,
          broadCastRightCMS.value.frequency(elem._1).estimate,
          numPartitions)
        getReplicationFactors(random, rightReplication, leftReplication).map(lr => ((elem._1, lr), elem._2))
      }
    }

    rddSkewed.cogroup(otherSkewed, partitioner).map(kv => (kv._1._1, kv._2))
  }

  def leanCoGroup[W: ClassTag](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] =
    leanCoGroup(other, defaultPartitioner(rdd, other))


  def leanJoin[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner,
                            leanReplication: leanReplication =  defaultLeanReplication(), cmsParam: CMSPara = CMSPara())
  : RDD[(K, (V, W))] =
    leanCoGroup(other, partitioner, leanReplication, cmsParam).flatMap { pair =>
      for (part1 <- pair._2._1.iterator; part2 <- pair._2._2.iterator) yield
        (pair._1, (part1, part2))
    }

  def leanJoin[W: ClassTag](other: RDD[(K, W)]): RDD[(K, (V, W))] =
    leanJoin(other, defaultPartitioner(rdd, other))

  def leanLeftOutJoin[W: ClassTag](ot: RDD[(K, W)], partitioner: Partitioner,
                                     leanReplications: leanReplication =  defaultLeanReplication(), cmsParam: CMSPara = CMSPara()):
  RDD[(K, (V, Option[W]))] =
    leanCoGroup(ot, partitioner, rightReplication(leanReplications), cmsParam).flatMap{
      case (k, (itv, Seq())) => itv.iterator.map(v => (k, (v, None)))
      case (k, (itv, itw)) => for (v <- itv; w <- itw) yield (k, (v, Some(w)))
    }

  def leanLeftOutJoin[W: ClassTag](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] =
    leanLeftOutJoin(other, defaultPartitioner(rdd, other))


  def leanRightOutJoin[W: ClassTag](ot: RDD[(K, W)], partitioner: Partitioner,
                                    leanReplications: leanReplication =  defaultLeanReplication(), cmsParam: CMSPara = CMSPara()):
  RDD[(K, (Option[V], W))] =

    leanCoGroup(ot, partitioner, leftReplication(leanReplications), cmsParam).flatMap{
      case (k, (Seq(), itw)) => itw.iterator.map(w => (k, (None, w)))
      case (k, (itv, itw)) => for (v <- itv; w <- itw) yield (k, (Some(v), w))
    }

  def leanRightOutJoin[W: ClassTag](ot: RDD[(K, W)]): RDD[(K, (Option[V], W))] =
    leanRightOutJoin(ot, defaultPartitioner(rdd, ot))
}