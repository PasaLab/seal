package edu.nju.pasalab.mt.wordAlignment.wordprep

import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap
import scala.collection.JavaConverters._
/**
  * Created by YWJ on 2016/6/11.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
class TypeTokenEstimator {
  var entryLimit = 0
  var hs : Long2ShortOpenHashMap = null
  var typeNum: Array[Double] = null
  var tokenNum: Array[Double] = null
  var limitHit: Boolean = false
  var step: Long = 0
  var tokens: Long = 0
  var totalStep: Int = 100

  def this(entryNum : Int) {
    this()
    this.entryLimit = entryNum
    typeNum = new Array[Double](totalStep + 1)
    tokenNum = new Array[Double](totalStep + 1)
    step = entryNum / (totalStep + 1)
    hs = new Long2ShortOpenHashMap((entryNum / 0.9).toInt)
  }
  def isLimitHit() : Boolean = limitHit

  var a: Double = 0
  var b: Double = 0
  val singleSide: Long = 0x40000000L
  var sz: Int = 0

  def justAddToken(n: Int) : Unit = {
    tokens += n
  }

  def newToken(src: Int, tgt: Int) : Unit = {
    tokens += 1
    if (!limitHit) {
      val id: Long = (src + 1) * singleSide + tgt
      val originalSize: Int = hs.size
      hs.put(id, 0.toShort)
      if (originalSize != hs.size && hs.size % step == 0) {
        typeNum(sz) = hs.size
        tokenNum(sz) = tokens
        sz += 1
      }
      if (hs.size == entryLimit) {
        limitHit = true
        val xbar: Double = mean(tokenNum, sz)
        val ybar: Double = mean(typeNum, sz)
        val norm: Double = dotProduct(tokenNum, typeNum, xbar, ybar, sz)
        val denorm: Double = dotProduct(tokenNum, tokenNum, xbar, xbar, sz)
        a = norm / denorm
        b = ybar - xbar * a
        hs = null
      }
    }
  }

  def mean(v: Array[Double], e: Int): Double = {
    var total: Double = 0
    for (e <- v)
      total += e

    total / e
  }

  def dotProduct(x: Array[Double], y: Array[Double], xbar: Double, ybar: Double, e: Int): Double = {
    var sum: Double = 0
    for (i <- 0 until e) {
      sum += (x(i) - xbar) * (y(i) - ybar)
    }
    sum
  }

  def getTypes: Double = {
    var result : Option[Double] = None
    if (!limitHit) {
      result = Some(hs.size)
    }
    else {
      result = Some(tokens * a + b)
    }
    result.get
  }
}
