package edu.nju.pasalab.mt.extraction.util.dependency

import java.io.PrintWriter
import scala.io.Source
object MSTParseReader {
  def changeFormat(inputFile:String, outputFile:String, textFile:String): Unit = {
    val brInput = Source.fromFile(inputFile, "utf-8").bufferedReader()
    val brText = Source.fromFile(textFile, "utf-8").bufferedReader()
    val pw = new PrintWriter(outputFile, "utf-8")
    var lineCount = 0
    var newline = true
    var dt:DependencyTree = null
    while (brInput.ready()) {
      var input = brInput.readLine()
      while (input.startsWith(" ") || input.startsWith("(")|| input.length == 0) {
        newline = true
        if (brInput.ready())
          input = brInput.readLine()
        else {
          input = null
          return
        }
      }
      if (newline) {
        lineCount += 1
        var text:String = null
        if (dt != null)
          pw.println(dt.toString())
        if (brText.ready())
          text = brText.readLine()
        else
          return
        val words:Array[String] = text.split(" ")
        dt = new DependencyTree(words)
        newline = false
      }
     // val dn:DependencyNode = MSTStr2Node(input)
      dt.addDenpency(MSTStr2Node(input))
    }
    brInput.close()
    brText.close()
    pw.close
  }
  //1	印尼	印尼	NR	NR	-	2	VMOD	-	-
  def MSTStr2Node(item:String): DependencyNode = {
    var dn:DependencyNode = null
    val parts = item.split("\t")
    dn = new DependencyNode()
    dn.label(7)
    dn.parent = Integer.parseInt(parts(6)) - 1
    dn.index = Integer.parseInt(parts(0)) - 1
    dn
  }
  /*def main(args:Array[String]): Unit = {
    val prefix:String =
  }*/
}
