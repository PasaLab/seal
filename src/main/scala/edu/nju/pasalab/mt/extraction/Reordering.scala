package edu.nju.pasalab.mt.extraction

/**
  * Created by dong on 2015/6/6.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object Reordering {


  def parseOrder(reorderInfo : String) : Array[Int] = {
    val resultArr = new Array[Int](6)

    if(reorderInfo.length > 3){
      val reorderArr = reorderInfo.trim().split("\\s+")
      for(i <- 0 to 5)
        resultArr(i) = reorderArr(i).toInt
    } else if(reorderInfo.length > 0) {

      if(reorderInfo(0) == 'm') resultArr(0) = 1
      else if(reorderInfo(0) == 's') resultArr(1) = 1
      else if(reorderInfo(0) == 'o') resultArr(2) = 1

      if(reorderInfo(2) == 't') resultArr(3) = 1
      else if(reorderInfo(2) == 'n') resultArr(4) = 1
      else if(reorderInfo(2) == 'p') resultArr(5) = 1
    } else{
      try{
        resultArr(0)
      }catch{
        case e:StringIndexOutOfBoundsException=>throw new Exception(reorderInfo)
      }
    }
    resultArr
  }

  def combineOrder(reorderA:String, reorderB:String):String={
    val aOrder = parseOrder(reorderA)
    val bOrder = parseOrder(reorderB)
    for(i <- 0 to 5)
      aOrder(i) += bOrder(i)
    aOrder.mkString(" ")
  }

  def formatOrder(reorder:String):String = {
    val reorderArr = parseOrder(reorder)

    val score = reorderArr.map(x => x + 1.0)
    //此处加1.0的原因是chaski中initOrder的时候有个加1.0，不明原因，先挪过来_应该是用于平滑
    var v = 0.0
    for(i <- 0 until 3)
      v += score(i)
    if(v == 0){
      score(0) = 1.0
      v = 1.0
    }
    for(i <- 0 until 3)
      score(i) /= v
    v = 0.0
    for(i <- 3 until 6)
      v += score(i)
    if(v == 0){
      score(3) = 1.0
      v = 1.0
    }
    for(i <- 3 until 6)
      score(i) /= v
    score.mkString(" ")
  }

}
