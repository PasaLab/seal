package edu.nju.pasalab.util

import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.ClassTag

/**
  * Created by YWJ on 2016.12.19.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */

trait tupleEncoders extends Serializable {

  implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)

  implicit def tuple2[A1, A2](implicit e1: Encoder[A1], e2: Encoder[A2]):
  Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]):
  Encoder[(A1,A2,A3)] = Encoders.tuple[A1,A2,A3](e1, e2, e3)

  implicit def tuple4[A1, A2, A3, A4](implicit e1 : Encoder[A1], e2 : Encoder[A2], e3 : Encoder[A3], e4 : Encoder[A4]) :
  Encoder[(A1,A2,A3,A4)] = Encoders.tuple[A1,A2,A3,A4](e1,e2,e3,e4)


  //......
}
