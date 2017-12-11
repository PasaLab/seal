package edu.nju.pasalab.mt.extraction.util.dependency

import java.io.PrintWriter
import java.util.regex.{Matcher, Pattern}

import scala.io.Source

/*
 *  1	However	RB	0/ROOT
		2	,	,	1/P
		3	restricted	VBN	1/COORD
		4	by	IN	3/VMOD
		5	communications	NNS	8/NMOD
		6	and	CC	8/COORD
		7	telecommunication	NN	8/COORD
		8	conditions	NNS	4/PMOD
		9	,	,	8/P
	*/

/**
 * This class is used to process the dependency format (tab format) produced by pennconvertor.
 * Each word is on a separator line, labeled with its index, lexicon, POS and head/relation.
 */
object TabFormatReader {
  val tabRegex = "(\\d+)\\t(\\S+)\\t(\\S+)\\t(\\d+)/(\\S+)"
  val tabPattern:Pattern = Pattern.compile(tabRegex)
  //    1	However	RB	0/ROOT
  //    2	,	,	1/P
  //    3	restricted	VBN	1/COORD
  //    4	by	IN	3/VMOD
  //    5	communications	NNS	8/NMOD
  //    6	and	CC	8/COORD
  //    7	telecommunication	NN	8/COORD
  //    8	conditions	NNS	4/PMOD
  //    9	,	,	8/P
  def tabStr2Node(item:String ): DependencyNodeWithPos ={
    val node = new DependencyNode()
    var PoS : String = null
    val mat: Matcher = tabPattern.matcher(tabRegex)
    if (mat.matches()) {
      node.index = mat.group(1).toInt - 1
      node.word = mat.group(2)
      PoS = mat.group(3)
      node.parent = mat.group(4).toInt - 1
      node.label = mat.group(5)
    }
    (new DependencyNodeWithPos(node, PoS))
  }
  /*def changeFormat(inputFile:String, outputFile:String, textFile:String, outputPoSFile:String): Unit = {
    val brDepLine = Source.fromFile(inputFile, "utf-8").bufferedReader()
    val brText = Source.fromFile(textFile, "utf-8").bufferedReader()
    val pwPoS = new PrintWriter(outputPoSFile, "utf-8")
    val pw = new PrintWriter(outputFile, "utf-8")
    val dep = new StringBuilder
    val pos = new StringBuilder
    val sent = new StringBuilder
    val text =
  }*/
  class DependencyNodeWithPos (val node2: DependencyNode, val PoS2:String) {
    val node:DependencyNode = null
    val Pos:String = null
  }
}