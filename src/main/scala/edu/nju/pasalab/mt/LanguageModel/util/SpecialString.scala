package edu.nju.pasalab.mt.LanguageModel.util

/**
  * Created by wuyan on 2015/12/24.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
object SpecialString {
  val mainSeparator:String = " ||| "
  val fileSeparator:String = "\t"
  val nullWordString:String = "<NULL>"
  val nullAlignString:String= "0-0"
  val secondarySeparator = " |||| "
  val listItemSeparator = " "
}

object Splitter{
  val mainSplitter = " \\|\\|\\| "
}
