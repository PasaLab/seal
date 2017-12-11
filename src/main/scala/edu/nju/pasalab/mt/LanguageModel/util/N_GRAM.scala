package edu.nju.pasalab.mt.LanguageModel.util

/**
  * Created by YWJ on 2015/12/28.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
case class N_GRAM(words: String, num: Long)

case class N_GRAM1(words: String , num : Double)

case class UNI_GRAM(words: String, prob: Double)

case class PREFIX(words: String, num: String)

case class Template(num : Long, arr: Array[Long])

case class MKN_NK(words: String, template: Template)

