package edu.nju.pasalab.util

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.apache.spark.Partitioner

/**
  * Created by YWJ on 2017.4.15.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
class pairPartitioner(numPart : Int) extends Partitioner {
  override def numPartitions : Int = numPart

  override def getPartition(key : Any) : Int = {
    //key.asInstanceOf[(String, String)]

    val x = convertFun(key)
    val code = shardForKey(key.toString) % numPartitions
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

  def shardForKey(str : String) : Int = {
    import scala.util.hashing.MurmurHash3._
    stringHash(str)
    //Hashing.farmHashFingerprint64().newHasher().putString(str, Charsets.UTF_8).hash().hashCode()
  }

  def convertFun(key : Any) = {
    key match {
      case tuple @ (a : Any, b : Any) => tuple
//      case tuple @(a :Any) => tuple
//      case tuple @ (a : Any, b : Any, c :Any) => tuple
    }
  }
}
