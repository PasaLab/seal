package edu.nju.pasalab.mt.LanguageModel.util

import org.apache.spark.sql.types._

/**
  * Created by wuyan on 2016/3/2.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
object Schema {
  def GT_Schema = StructType(
    Array(StructField("ID", LongType, true),
      StructField("count", DoubleType, true)
    ))

  def NGRAM_Join = StructType(
    Array(StructField("words", StringType, true),
      StructField("prefix_num", LongType, true),
      StructField("num", LongType, true)
    ))
}
