package edu.nju.pasalab.mt.wordAlignment.wordprep

import java.io._
import edu.nju.pasalab.mt.util.CommonFileOperations
import edu.nju.pasalab.mt.wordAlignment.programLogic
import CommonFileOperations.openFileForRead
import CommonFileOperations.openFileForWrite
import  programLogic.testFileExistOnHDFS
import org.apache.commons.logging.{LogFactory, Log}

/**
  * Created by YWJ on 2016/6/11.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object merged {
  private val LOG: Log = LogFactory.getLog(merged.getClass)
  val sep: CharSequence = "|||"
  val sepReplacement: CharSequence = "-S:P-"
  val sepOut: String = " ||| "
  def execute(inputSource: String, inputTarget : String ,  mergedCorpus : String, maxSentLen: Int, encoding : String): Unit = {
    val srcOnHDFS: Boolean = testFileExistOnHDFS(inputSource, "File is not on HDFS, try local", LOG)
    val tgtOnHDFS: Boolean = testFileExistOnHDFS(inputTarget, "File is not on HDFS, try local", LOG)

    val srcIn: InputStream = openFileForRead(inputSource, srcOnHDFS)
    val tarIn: InputStream = openFileForRead(inputTarget, tgtOnHDFS)
    val mergeOut: OutputStream = openFileForWrite(mergedCorpus, true)
    val sin: BufferedReader = new BufferedReader(new InputStreamReader(srcIn, encoding))
    val tin: BufferedReader = new BufferedReader(new InputStreamReader(tarIn, encoding))

    val out: PrintWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(mergeOut, "UTF-8"), 65 * 1024 * 1024))
    var i = 0
    var exit = false
    while (!exit) {
      val s1 = sin.readLine()
      val t1 = tin.readLine()
      if (!(s1 == null || t1 == null )) {
        if (maxSentLen > 0) {
          if (s1.trim.length != 0 && t1.trim.length != 0) {
            val sw = s1.trim.split("\\s+")
            val tw = t1.trim.split("\\s+")
            if (!(sw.length > maxSentLen || tw.length > maxSentLen)) {
              val src1 = s1.replace(sep, sepReplacement)
              val tgt1 = t1.replace(sep, sepReplacement)
              i += 1
              //why keep this ?
//              out.print(i)
//              out.print("\t")
              out.print(src1)
              out.print((sepOut))
              out.println(tgt1)
            }
          }
        }
      } else
        exit = true
    }
    out.close()
  }
}
