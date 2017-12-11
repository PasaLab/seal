package edu.nju.pasalab.util

import com.google.common.base.{Charsets, Splitter}
import com.google.common.hash.{Hashing, HashFunction}


import org.apache.spark.Partitioner

/**
  * Created by YWJ on 2017.3.28.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
class gramPartitioner(numPart : Int) extends Partitioner {

  override def numPartitions : Int = numPart

  override def getPartition(key : Any) : Int = {
    val code = shardForGarm(key.toString) % numPartitions
    if (code < 0)
      code + numPartitions
    else
      code

  }

  override def equals(ot : Any) : Boolean = ot match {
    case ite : gramPartitioner => ite.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode : Int = numPartitions

  //firstTwoWords
  def shardForGarm(str : String) : Int = {
    val sp = str.trim.split(" ")
    val sb = new StringBuilder(64)
    sb.append(sp(0)).append(" ").append(sp(1))

    Hashing.farmHashFingerprint64().newHasher().putString(sb.toString(), Charsets.UTF_8).hash().hashCode()

    // Hashing.murmur3_128().newHasher().putString(sb.toString(), Charsets.UTF_8).hash().hashCode()
    // import scala.util.hashing.MurmurHash3._
    // stringHash(sb.toString())
  }
}
