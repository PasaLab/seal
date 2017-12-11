package edu.nju.pasalab.mt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by YWJ on 2017.4.16.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object testIterator {
  def main(args: Array[String]) {

    //val logger  = LoggerFactory.getLogger(test.getClass)

    System.setProperty("hadoop.home.dir", "D:\\workspace\\winutils")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val rdd1 = spark.sparkContext.makeRDD(1 to 10, 2)
    val ite = Iterator(1,2,3,4,5,6,7,8,9,10)

    var sum = 0
    while(ite.hasNext){
      sum += ite.next
    }
    println("sum is " + sum)
    val expression1 = if(ite.isEmpty) "iterator is empty" else "iterator is not empty"
    println(expression1)

    val x = Array(1 to 10)
    x.foreach(println)


    spark.stop()
  }
}
