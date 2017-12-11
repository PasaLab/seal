import java.io.File

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.common.primitives.Floats
import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.util.configRead
import org.apache.log4j.{Level, Logger}

import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by YWJ on 2017.3.9.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object testKey {
  def main(args: Array[String]) {
    val key1 = "我 爱 中 国"
    val key2 = "Football is very interesting"
    val index = key2.indexOf(" ")
    println(key2.substring(index+1))
    //println(funX(shardForGarm(key1) % 192, 192) + "\t" + funX(shardCityHash(key1) % 192, 192))
   // println(funX(shardForGarm(key2) % 192, 192) + "\t" + funX(shardCityHash(key2) % 192, 192))


    println(191 / 2)
    //com.google.common.math.DoubleMath
   // BlockMan
  }


  def shardForGarm(str : String) : Int = {
    //val sp = Splitter.on(" ").trimResults().omitEmptyStrings().split(str).iterator()
    val sp = str.trim.split(" ")
    val sb = new StringBuilder(64)
    //firstTwoWords
    sb.append(sp(0)).append(" ").append(sp(1))
    //import scala.util.hashing.MurmurHash3._
    //stringHash(sb.toString())
    //com.google.common.hash.Murmur3_32HashFunction
    //com.google.common.hash.Murmur3_128HashFunction
    Hashing.murmur3_128().newHasher().putString(sb.toString(), Charsets.UTF_8).hash().hashCode()
  }


  def funX(code : Int, numPartitions : Int) : Int = {
    if (code < 0)
      code + numPartitions
    else
      code
  }
}
