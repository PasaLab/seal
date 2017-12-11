package edu.nju.pasalab.util

import org.apache.spark.sql.Encoders

/**
  * Created by YWJ on 2016.12.31.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
trait customEncoders {

  implicit val mapEncoder = Encoders.kryo[Map[String, Int]]

  //implicit val tuple2 = Encoders.kryo[Tuple2[]]
}
