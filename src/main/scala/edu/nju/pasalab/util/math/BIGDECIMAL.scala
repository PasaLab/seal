package edu.nju.pasalab.util.math

/**
  * Created by YWJ on 2016/3/1.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
object BIGDECIMAL {
  def add(v1 : Double, v2: Double): Double = {
    val b1 : BigDecimal = BigDecimal(v1)
    val b2 : BigDecimal = BigDecimal(v2)
    (b1 + b2).doubleValue()
  }

  def subtract(v1 : Double, v2: Double): Double = {
    val b1 : BigDecimal = BigDecimal(v1)
    val b2 : BigDecimal = BigDecimal(v2)
    (b1 - b2).doubleValue()
  }

  def multiply (v1 : Double, v2: Double): Double = {
    val b1 : BigDecimal = BigDecimal(v1)
    val b2 : BigDecimal = BigDecimal(v2)
    (b1 * b2).doubleValue()
  }

  def divide (v1 : Double, v2: Double): Double = {
    val b1 : BigDecimal = BigDecimal(v1)
    val b2 : BigDecimal = BigDecimal(v2)
    (b1 / b2).doubleValue()
  }

  def max (v1 : Double, v2: Double): Double = {
    val b1 : BigDecimal = BigDecimal(v1)
    val b2 : BigDecimal = BigDecimal(v2)
    b1.max(b2).doubleValue()
  }

  def min(v1 : Double, v2: Double): Double = {
    val b1 : BigDecimal = BigDecimal(v1)
    val b2 : BigDecimal = BigDecimal(v2)

    b1.min(b2).doubleValue()
  }
}
