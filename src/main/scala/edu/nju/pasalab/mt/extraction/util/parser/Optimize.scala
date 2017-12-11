package edu.nju.pasalab.mt.extraction.util.parser

import edu.nju.pasalab.mt.extraction.util.parser.NLPConfig.optimize
import edu.stanford.nlp.optimization.{QNMinimizer, DiffFunction}
import NLPConfig._
import edu.nju.pasalab.mt.extraction.util.parser.Optimize._
/**
  * Created by YWJ on 2016.12.25.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
// ----------
// Optimizers
// ----------
object Optimize {
  def empiricalDerivative(fn:Array[Double]=>Double,
                          x:Array[Double]):Array[Double] = {
    val y0 = fn(x)
    def tweak(i:Int, delta:Double):(Double, Double) = {
      x(i) += delta
      val y1 = fn(x)
      x(i) -= delta
      if (delta < 1e-5 * optimize.wiggle || delta > 1e5 * optimize.wiggle) {
        (y1, delta)
      } else {
        if (scala.math.abs(y1 - y0) / delta > 1e5) tweak(i, delta / 2.0)
        else if (scala.math.abs(y1 - y0) / delta < 1e-5) tweak(i, delta * 2.0)
        else (y1, delta)
      }
    }
    {for (i <- 0 until x.length) yield {
      val (y1, step) = tweak(i, optimize.wiggle)
      (y1 - y0) / step
    }}.toArray
  }
}

trait OptimizableFunction {
  def minimize(initial:Array[Double]):Array[Double]
  def derivative(ddx:Array[Double]=>Array[Double]):OptimizableFunction
}

/**
  * A wrapper for QNMinimizer (L-BFGS)
  */
case class LBFGSOptimizableApproximateFunction(
                                                fn:Array[Double]=>Double, derivative:Option[Array[Double]=>Array[Double]])
  extends OptimizableFunction{

  override def minimize(initial:Array[Double]):Array[Double] = {
    // (define a differentiable function)
    val javaFn:DiffFunction = new DiffFunction {
      override def domainDimension:Int = initial.length
      override def valueAt(x:Array[Double]):Double = fn(x)
      override def derivativeAt(x:Array[Double]):Array[Double] = {
        derivative match {
          case Some(ddx) => ddx(x)
          case None => empiricalDerivative(fn, x)
        }
      }
    }
    // (optimize using QNMinimizer)
    val javaInit = initial.map{ (n:Double) => n }
    val optimizer = new QNMinimizer()
    optimizer.setRobustOptions()
    optimizer.minimize(javaFn, optimize.tolerance, javaInit)
  }

  override def derivative(ddx:Array[Double]=>Array[Double]):LBFGSOptimizableApproximateFunction
  = new LBFGSOptimizableApproximateFunction(fn, Some(ddx))
}

/**
  * An optimization algorithm I made up (thus, "braindead"), that tries its
  * best to move against the gradient (thus, "gradient descent").
  * The only motivation to use this over L-BFGS is that it's more robust to
  * non-convex problems (i.e., won't crash and burn).
  */
case class BraindeadGradientDescent(
                                     fn:Array[Double]=>Double, derivative:Option[Array[Double]=>Array[Double]])
  extends OptimizableFunction{

  override def minimize(initial:Array[Double]):Array[Double] = {
    // (helpers)
    def dx(x:Array[Double], y0:Double):Array[Double] = derivative match {
      case Some(ddx) => ddx(x)
      case None => empiricalDerivative(fn, x)
    }
    def move(init:Array[Double], direction:Array[Double], scaling:Double):Array[Double] = {
      init.zip(direction).map{ case (a:Double, d:Double) => a + scaling * d}
    }
    def isImprovementOver(newY:Double, y:Double):Boolean
    = newY + optimize.tolerance < y
    // (state)
    val initialX:Array[Double] = initial
    val initialY:Double        = fn(initialX)
    var x:Array[Double]        = initialX
    var y:Double               = initialY
    var numIters = 0
    // (optimization)
    while (numIters < 100) {
      var step:Double        = 1.0
      val dir:Array[Double]  = dx(x, y).map( - _ )
      var newX:Array[Double] = move(x, dir, step)
      var newY:Double        = fn(newX)
      while (!isImprovementOver(newY, y) && step > 1e-5) {
        step /= 2.0
        newX = move(x, dir, step)
        newY = fn(newX)
      }
      if (step <= 1e-5) return x // convergence
      assert(newY < y, "Function value did not decrease!")
      x = newX
      y = newY
      numIters += 1
    }
    // (timeout -- no convergence)
   x
  }

  override def derivative(ddx:Array[Double]=>Array[Double]):BraindeadGradientDescent
  = new BraindeadGradientDescent(fn, Some(ddx))
}

