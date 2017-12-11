package edu.nju.pasalab.util.math

import java.math.BigDecimal
//import java.math.BigInteger

/**
  * Created by YWJ on 2017.4.17.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object bigDecimalOp {

  def ln(a : BigDecimal, scale : Int) : BigDecimal = {
    // check a > 0
    if (a.signum <= 0) throw new IllegalArgumentException("a <= 0")

    // the number of digits to the left of the decimal point.
    val magnitude : Int = a.toString.length - a.scale - 1

    if (magnitude < 3) lnNewTon(a, scale)
    else {
      // x^(1/magnitude)
      val root : BigDecimal = intRoot(a, magnitude, scale)
      // ln(x^(1/magnitude))
      val lnRoot : BigDecimal = lnNewTon(root, scale)
      // magnitude*ln(x^(1/magnitude))
      BigDecimal.valueOf(magnitude).multiply(lnRoot).setScale(scale, BigDecimal.ROUND_HALF_EVEN)
    }
  }

  def lnNewTon(a : BigDecimal, scale : Int) : BigDecimal = {
    val sp : Int = scale + 1
    val n : BigDecimal = a
    var term : BigDecimal = n
    var x : BigDecimal = a
    // Convergence tolerance = 5*(10^-(scale+1))
    val tolerance : BigDecimal = BigDecimal.valueOf(5).movePointLeft(sp)

    do {
      // e^x
      val eToX : BigDecimal = exp(a, sp)
      // (e^x - n)/e^x
      term = eToX.subtract(n).divide(eToX, sp, BigDecimal.ROUND_DOWN)
      // x - (e^x - n)/e^x
      x = x.subtract(term)
      Thread.`yield`()
    } while (term.compareTo(tolerance) > 0)

    x.setScale(scale, BigDecimal.ROUND_HALF_EVEN)
  }

  /**
    * Compute the int root of x to a given scale
    * @param a
    * @param index
    * @param scale
    * @return
    */
  def intRoot(a : BigDecimal, index : Long, scale : Int) : BigDecimal = {
    // check x >= 0
    if (a.signum() < 0) throw new IllegalArgumentException("a < 0")
    val sp : Int = scale + 1
    val n : BigDecimal = a
    val i : BigDecimal = BigDecimal.valueOf(index)
    val im1 : BigDecimal = BigDecimal.valueOf(index-1)
    // Convergence tolerance = 5*(10^-(scale+1))
    val tolerance : BigDecimal = BigDecimal.valueOf(5).movePointLeft(sp)

    var xPrev : BigDecimal = a
    var x : BigDecimal = a
    //init approximation is : x/index
    x = x.divide(i, scale, BigDecimal.ROUND_HALF_EVEN)
    do {
      // x^(index-1)
      val xTolm : BigDecimal = intPower(a, index - 1, sp)

      // x^index
      val xTo = x.multiply(xTolm).setScale(sp, BigDecimal.ROUND_HALF_EVEN)
      // n + (index-1)*(x^index)
      val numerator = n.add(im1.multiply(xTo)
          .setScale(sp, BigDecimal.ROUND_HALF_EVEN))
      // (index * (x^(index-1))
      val denominator = i.multiply(xTolm
        .setScale(sp, BigDecimal.ROUND_HALF_EVEN))

      xPrev = x
      x = numerator.divide(denominator, sp, BigDecimal.ROUND_DOWN)
      Thread.`yield`()
    } while (x.subtract(xPrev).abs().compareTo(tolerance) > 0)
    x
  }

  /**
    * Compute a^exp^ to a given scale
    * @param a
    * @param exp
    * @param scale
    * @return
    */
  def intPower(a : BigDecimal, exp : Long, scale : Int) : BigDecimal = {
    if (exp < 0)
      return BigDecimal.valueOf(1).divide(intPower(a, -exp, scale), scale, BigDecimal.ROUND_HALF_EVEN)
    var power : BigDecimal = BigDecimal.valueOf(1)
    var x : BigDecimal = a
    var exp1 = exp
    while (exp1 > 0) {
      // Is the rightmost bit is 1?
      if ((exp1 & 1) == 1) {
        power = power.multiply(x).setScale(scale, BigDecimal.ROUND_HALF_EVEN)
      }

      // Square x and shift exp 1 bit to the right
      x = x.multiply(x).setScale(scale, BigDecimal.ROUND_HALF_EVEN)

      exp1 >>= 1

      Thread.`yield`()
    }
    power
  }

  /**
    * Compute e^a^
    * Break a into its whole and fraction parts and
    * compute (e ^ (1 + fraction/whole))^whole using Taylor's formula
    * @param x
    * @param scale
    * @return
    */
  def exp(x : BigDecimal, scale : Int) : BigDecimal = {
    if (x.signum() == 0) return BigDecimal.valueOf(1)
    else if (x.signum() == -1)
      return BigDecimal.valueOf(1).divide(exp(x.negate(), scale), scale, BigDecimal.ROUND_HALF_EVEN)

    //compute thw whole part of x
    var xWhole = x.setScale(0, BigDecimal.ROUND_DOWN)

    // if there isn't a whole part, compute e^x
    if (xWhole.signum() == 0) return expTaylor(x, scale)

    // compute the fraction part of x
    val xFraction = x.subtract(xWhole)

    //z = 1 + fraction/whole
    val z : BigDecimal = BigDecimal.valueOf(1)
      .add(xFraction.divide(xWhole, scale, BigDecimal.ROUND_HALF_EVEN))
    // t = e^z

    val t : BigDecimal = expTaylor(z, scale)

    val maxLong : BigDecimal = BigDecimal.valueOf(Long.MaxValue)
    var res : BigDecimal = BigDecimal.valueOf(1)

    // Compute t^whole
    // if whole > Long.MaxValue, then first compute product

    while (xWhole.compareTo(maxLong) >= 0) {
      res = res.multiply(intPower(t, Long.MaxValue, scale))
        .setScale(scale, BigDecimal.ROUND_HALF_EVEN)
      xWhole = xWhole.subtract(maxLong)
      Thread.`yield`()
    }
    res.multiply(intPower(t, xWhole.longValue(), scale))
      .setScale(scale, BigDecimal.ROUND_HALF_EVEN)
  }

  /**
    *
    * @param x
    * @param scale
    * @return
    */
  def expTaylor(x : BigDecimal, scale : Int) : BigDecimal = {
    var factorial : BigDecimal = BigDecimal.valueOf(1)
    var xPower : BigDecimal = x
    var sumPerv : BigDecimal = BigDecimal.valueOf(0)

    var sum : BigDecimal = x.add(BigDecimal.valueOf(1))

    var i = 2
    do {
      // x^i
      xPower = xPower.multiply(x).setScale(scale, BigDecimal.ROUND_HALF_EVEN)
      // i!
      factorial = factorial.multiply(BigDecimal.valueOf(i))
      // x^i/i!
      val tmp : BigDecimal = xPower.divide(factorial, scale,
        BigDecimal.ROUND_HALF_EVEN)

      // sum = sum + x^i/i!
      sumPerv = sum
      sum = sum.add(tmp)
      i = i + 1
      Thread.`yield`()
    }while (sum.compareTo(sumPerv)  != 0)
    sum
  }
}
