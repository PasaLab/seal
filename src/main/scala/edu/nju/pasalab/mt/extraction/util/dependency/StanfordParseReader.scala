package edu.nju.pasalab.mt.extraction.util.dependency

import java.io.PrintWriter
import java.util.regex.{Matcher, Pattern}
import scala.io.Source

object StanfordParseReader {
  def changeFormat(inputFile:String, outputFile:String, textFile:String): Unit ={
    val brInput = Source.fromFile(inputFile, "utf-8").bufferedReader()
    val brText = Source.fromFile(textFile, "utf-8").bufferedReader()
    val pw = new PrintWriter(outputFile, "utf-8")
    var lineCount = 0
    var treeLineCount = 0
    var emptyLineCount = 0
    var dt :DependencyTree = null
    var words:Array[String] = null
    var first:Boolean = true
    var text:String = null
    while (brInput.ready()) {
      var input = brInput.readLine()
      treeLineCount += 1
      val judge = (input.startsWith("(") || input.length == 0)
      if (input.startsWith("(") || input.startsWith("Sentence skipped")) {
        if (dt != null) {
          pw.println(dt.toString())
          dt = null
        }
        else if(!first) {
          pw.println()
          first = false
          emptyLineCount += 1
        }
        var foundInt = false
        while(brText.ready() && !foundInt) {
          text= brText.readLine()
          lineCount += 1
          if (lineCount == 117470) //why this number?
            println("Stop")
          /*if (text.equals("")) {
            pw.println()
            emptyLineCount += 1
            //continue
          }*/
          if (! text.equals("")){
            words = text.split("?| |　")
            dt = new DependencyTree(words)
            if (words.length == 1)
              dt.addDenpency(stanfordStr2Node("root(ROOT-0, " + words(0) + "-1)"))
            foundInt = true
          }
          pw.println()
          emptyLineCount += 1
        }
      } else if(input.equals("SENTENCE_SKIPPED_OR_UNPARSABLE")) {
        dt = null
      } else if (!judge) {
        dt.addDenpency(stanfordStr2Node(input))
      }
    }
    if (dt != null)
      pw.println(dt.toString())
    else {
      pw.println()
      emptyLineCount += 1
    }
    brInput.close()
    brText.close()
    pw.close()
  }

  /**
   * static String stanfordregex="(\\S+)\\(\\S+-(\\d+),\\s(\\S+)-(\\d+)\\)";
   * static Pattern stanfordpattern=Pattern.compile(stanfordregex);
   */
  val stanfordRegex = "(\\\\S+)\\\\(\\\\S+-(\\\\d+),\\\\s(\\\\S+)-(\\\\d+)\\\\)"
  val stanfordPattern:Pattern = Pattern.compile(stanfordRegex)
  def stanfordStr2Node(str : String): DependencyNode = {
    val node:DependencyNode = new DependencyNode()
    val matcher: Matcher = stanfordPattern.matcher(str)
    if (matcher.matches()) {
      node.label = matcher.group(1)
      node.parent = matcher.group(2).toInt - 1
      node.index = matcher.group(4).toInt - 1
      node.word = matcher.group(3)
    }
    node
  }
}
