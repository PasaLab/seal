package edu.nju.pasalab.mt.extraction.util.alignment

import scala.collection.mutable.ArrayBuffer

class AlignmentTable(val rowNum: Int, val colNum: Int) {
  var matrix: Array[Array[Boolean]] = new Array(rowNum)
  for (r <- 0 until rowNum) {
    matrix(r) = new Array[Boolean](colNum)
    for (cell <- 0 until colNum)
      matrix(r)(cell) = false
  }
  var linkCount: Int = 0
  def this(rowNum: Int, colNum: Int, matrix: Array[Array[Boolean]], linkCount: Int) {
    this(rowNum, colNum)
    this.matrix = matrix
    this.linkCount = linkCount
  }
  /** *
    * form a align matrix for source words and target words
    * @param alignStr sentence string
    */
  def fillMatrix_aligntable(alignStr: String) {
    require(alignStr != null && !alignStr.equals(" "))
    val alignments = alignStr.trim.split(" ")
    var srcid= -1
    var tarid= -1
    var srcitem = ""
    var taritem = ""
    for (align <- alignments) {
      if (!align.equals("")) {
        val s: String = align.trim
        val index: Int = s.indexOf("-")
        srcitem = s.substring(0, index)
        taritem = s.substring(index + 1)
        srcid = srcitem.toInt
        tarid = taritem.toInt
        if (srcid < matrix.length && tarid < matrix(srcid).length && !matrix(srcid)(tarid)) {
          matrix(srcid)(tarid) = true
          linkCount += 1
        }
      }
    }
  }
  /** *
    * Get the target aligned words to the specific source word
    * @param index the index of source word
    * @return index array of the aligned target words
    */
  def getAlignWords_Src(index: Int): Array[Int] = {
    val result = ArrayBuffer[Int]()
    if (index < 0 || index >= rowNum)
      result.toArray
    else {
      for (i <- 0 until colNum) {
        if (matrix(index)(i))
          result += i
      }
      result.toArray
    }
  }
  /** *
    * Get the source aligned words to the specific target word
    * @param index the index of target word
    * @return index array of the aligned source words
    */
  def getAlignWords_Trg(index: Int): Array[Int] = {
    val result = ArrayBuffer[Int]()
    if (index < 0 || index >= colNum)
      result.toArray
    else {
      for (i <- 0 until rowNum)
        if (matrix(i)(index)) result += i
      result.toArray
    }
  }
  def isAligned(i:Int, j:Int): Boolean = {
    var res : Boolean = false
    if ((i == -1 && j == -1) || (i >= rowNum && j >= colNum))
      res = true

    if(i < rowNum && i >= 0 && j<colNum && j >= 0) {
      res = matrix(i)(j)
    } else{
      res = false
    }
    res
  }
  def isTrgAligned(index:Int):Boolean = {
    for(i<-0 until rowNum){
      if(matrix(i)(index))
        return true
    }
    false
  }
}
object AlignmentTable {
  def getReverseAlignStr(align: String): String = {
    var revAli: String = ""
    val cells: Array[String] = align.trim.split("\\s+")
    for (i <- cells.indices) {
      if (cells(i).length != 0) {
        val fields: Array[String] = cells(i).split("-")
        revAli += fields(1) + "-" + fields(0) + " "
      }
    }
    revAli.trim
  }
}