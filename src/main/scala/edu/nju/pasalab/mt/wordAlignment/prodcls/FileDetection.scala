package edu.nju.pasalab.mt.wordAlignment.prodcls

import java.io._
import chaski.utils.CorpusFilter
import edu.nju.pasalab.mt.util.CommonFileOperations
import edu.nju.pasalab.mt.wordAlignment.programLogic

import org.apache.commons.logging.Log
import programLogic.testFileExistOnHDFSOrDie
import programLogic.testFileNotExistOnHDFSOrDie
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by YWJ on 2016/6/6.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object FileDetection {
  val logger:Logger  = LoggerFactory.getLogger(FileDetection.getClass)
  def execute(nocopy : Boolean, overwrite: Boolean, source : String, srcInputCorpusHDFS: String, target : String, tgtInputCorpusHDFS :String ,
              srcOutput : String, tgtOutput : String, maxSentLen : Int, filterLog : String, LOG:Log): Unit = {

    val fs : FileSystem = FileSystem.get(new Configuration())

    if (nocopy) {
      //src
      testFileExistOnHDFSOrDie(srcInputCorpusHDFS, "File Not Found" + srcInputCorpusHDFS + " On HDFS ",
        (new FileNotFoundException).getClass, LOG)
      //tgt
      testFileExistOnHDFSOrDie(tgtInputCorpusHDFS, "File Not Found" + tgtInputCorpusHDFS + " On HDFS ",
        (new FileNotFoundException).getClass, LOG)
    } else {
      if (overwrite) {
        //deleteIfExists(srcInputCorpusHDFS)
        //deleteIfExists(tgtInputCorpusHDFS)
      } else {
        testFileNotExistOnHDFSOrDie(srcInputCorpusHDFS, "File Not Found" + srcInputCorpusHDFS + " On HDFS ",
          (new FileNotFoundException).getClass, LOG)
        testFileNotExistOnHDFSOrDie(tgtInputCorpusHDFS, "File Not Found" + tgtInputCorpusHDFS + " On HDFS ",
          (new FileNotFoundException).getClass, LOG)
      }

      if (!fs.exists(new Path(srcInputCorpusHDFS)) && !fs.exists(new Path(tgtInputCorpusHDFS))) {

        val srcIn : InputStream = new FileInputStream(source)
        val tgtIn : InputStream = new FileInputStream(target)
        val fLog : OutputStream = new FileOutputStream(filterLog)
        val srcOut : OutputStream = CommonFileOperations.openFileForWrite(srcInputCorpusHDFS, true)
        val tgtOut : OutputStream = CommonFileOperations.openFileForWrite(tgtInputCorpusHDFS, true)

        val flt : CorpusFilter = new CorpusFilter(maxSentLen)
        flt.filter(srcIn, tgtIn, srcOut, tgtOut, fLog)
        fLog.close()
        srcOut.close()
        tgtOut.close()
      }
    }
    if (overwrite) {
      if (fs.exists(new Path(srcOutput)))
        if (!fs.delete(new Path(srcOutput), true)) {
          System.err.println("can not delete file " + srcOutput)
          logger.info("can not delete file " + srcOutput, throw new IOException())
        }

      if (fs.exists(new Path(tgtOutput)))
        if (!fs.delete(new Path(tgtOutput), true)) {
          System.err.println("can not delete file " + tgtOutput)
          logger.info("can not delete file " + tgtOutput, throw new IOException())
        }
      //deleteIfExists(srcOutput)
      //deleteIfExists(tgtOutput)
    } else {
      testFileNotExistOnHDFSOrDie(srcOutput, "File Already Exists " + srcOutput + " On HDFS ",
        (new FileNotFoundException).getClass, LOG)
      testFileNotExistOnHDFSOrDie(tgtOutput, "File Already Exists " + tgtOutput + " On HDFS ",
        (new FileNotFoundException).getClass, LOG)
    }

  }
}
