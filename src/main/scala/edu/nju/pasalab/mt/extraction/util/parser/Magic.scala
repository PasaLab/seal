package edu.nju.pasalab.mt.extraction.util.parser

import scala.collection.Map
import NLPConfig._

import scala.reflect.ClassTag

/**
  * Created by YWJ on 2016.12.25.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object Magic {
  /*
   * Implicit Conversions
   */
  implicit def seq2nlpseq(seq:Seq[String]):Sentence = new Sentence(seq)
  implicit def string2nlpseq(gloss:String):Sentence = new Sentence(gloss)

  implicit def map2mapping[I : ClassTag, O: ClassTag, X : ClassTag](map:Map[I,X]):Mapping[I,O] = Mapping(map)

  implicit def seq2ensemble[I : ClassTag](seq:Seq[I=>Boolean]):Ensemble[I] = new Ensemble(seq, None)

  implicit def fn2optimizable(
                               fn:Array[Double]=>Double):OptimizableFunction = {
    optimize.algorithm.toLowerCase match {
      case "lbfgs" => LBFGSOptimizableApproximateFunction(fn, None)
      case "braindead" => BraindeadGradientDescent(fn, None)
      case _ => throw new IllegalStateException("Unknown algorithm: " + optimize.algorithm)
    }
  }
  implicit def fnPair2optimizable(
                                   pair:(Array[Double]=>Double,Array[Double]=>Array[Double])):OptimizableFunction = {
    optimize.algorithm.toLowerCase match {
      case "lbfgs" => LBFGSOptimizableApproximateFunction(pair._1, Some(pair._2))
      case "braindead" => BraindeadGradientDescent(pair._1, Some(pair._2))
      case _ => throw new IllegalStateException("Unknown algorithm: " + optimize.algorithm)
    }
  }

  implicit def string2tokensregex(str:String):TokensRegex = new TokensRegex(str)
}
