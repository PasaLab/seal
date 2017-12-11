package edu.nju.pasalab.mt.optimize.join

import scala.math.{min, max}

import org.slf4j.LoggerFactory
/**
  * Created by YWJ on 2016.12.18.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
trait leanReplication extends Serializable {
  def getReplication(left : Long, right : Long, numPartitions : Int) : (Int, Int)
}

case class defaultLeanReplication(replicationFactor : Double = 1e-2) extends leanReplication {
  override def getReplication(left : Long, right : Long, numPartitions : Int) : (Int, Int) = (
      max(min((right * replicationFactor).toInt, numPartitions), 1),
      max(min((left * replicationFactor).toInt, numPartitions), 1)
    )
}

private case class rightReplication(leanReplication : leanReplication) extends leanReplication {
  override def getReplication(left : Long, right : Long, numPartitions : Int) : (Int, Int) = {
    val (l, r) = leanReplication.getReplication(left, right, numPartitions)
    (1, max(min(l * r, numPartitions), 1))
  }
}

private case class leftReplication(leanReplication : leanReplication) extends leanReplication {
  override def getReplication(left: Long, right: Long, numPartitions: Int): (Int, Int) = {
    val (l, r) = leanReplication.getReplication(left, right, numPartitions)
    (max(min(l * r, numPartitions), 1), 1)
    //(left, 1)
  }
}

private object loggingLeanReplication {
  private val log = LoggerFactory.getLogger(getClass)
}

case class loggingLeanReplication(leanReplication : leanReplication) extends leanReplication {
  import loggingLeanReplication._
  private var maxLeftReplication = 0
  private var maxRightReplication = 0

  override def getReplication(left: Long, right: Long, numPartitions: Int): (Int, Int) = {
    val (l, r) = leanReplication.getReplication(left, right, numPartitions)
    if (l > maxLeftReplication) {
      log.info("new max left replication {}", left)
      maxLeftReplication = l
    }
    if (right > maxRightReplication) {
      log.info("new max right replication {}", right)
      maxRightReplication = r
    }
    (l, r)
  }
}


