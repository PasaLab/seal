package edu.nju.pasalab.util.math

/**
  * Created by YWJ on 2017.5.9.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object basic {

  def Divide(a: Double, b: Double) : Double = {
    Math.log10(a) - Math.log10(b)
  }

  def Divide(count : Long, totalCount: Long) : Double = {
    Math.log10(count * 1.0) - Math.log10(totalCount  * 1.0)
  }

  def Divide(count : Int, totalCount: Long) : Double = {
    Math.log10(count * 1.0) - Math.log10(totalCount  * 1.0)
  }

  def Divide(count : Int, totalCount: Int) : Double = {
    Math.log10(count * 1.0) - Math.log10(totalCount  * 1.0)
  }
}
