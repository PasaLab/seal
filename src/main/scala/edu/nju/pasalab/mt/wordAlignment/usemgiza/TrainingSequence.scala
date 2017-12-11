package edu.nju.pasalab.mt.wordAlignment.usemgiza

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util
import chaski.utils.{CommandSheet, BinaryToStringCodec}
import edu.nju.pasalab.mt.wordAlignment.util.{GIZAAlignmentTask, ExternalUtils}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import edu.nju.pasalab.util.ExtractionParameters
import org.apache.spark.HashPartitioner

/**
  * Created by YWJ on 2016/6/8.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
class TrainingSequence extends Serializable {


  val logger = LoggerFactory.getLogger(classOf[TrainingSequence])

  private var trainseq: String = null
  private var src2tgt: Boolean = false
  private var resumeIfPossible: Boolean = false

  def isResumeIfPossible: Boolean = {
    resumeIfPossible
  }

  def setResumeIfPossible(resumeIfPossible: Boolean) {
    this.resumeIfPossible = resumeIfPossible
  }

  private var rootDir: String = null
  private var gizaParas: Array[util.Map[String, String]] = null
  private var defaultGIZAPara: util.Map[String, String] = null
  private var memoryLimit: Int = 0

  def getGizaParas: Array[util.Map[String, String]] = {
    gizaParas
  }

  def setGizaParas(gizaParas: Array[util.Map[String, String]]) {
    this.gizaParas = gizaParas
  }

  private val seqVerify: Pattern = Pattern.compile("P?(1+\\*?)*(H+\\*?)*(3+\\*?)*(4+\\*?)*(\\*(1+\\*?)*(H+\\*?)*(3+\\*?)*(4+\\*?)*)*")

  /**
    *
    * @param seq        The sequence
    * @param src2tgt    if true, source-to-target model will be trained otherwise
    * @param generalRoot   rootDir
    * @param memoryLimit how much memory a partition have
    */
  def this(seq: String, src2tgt: Boolean, generalRoot: String, memoryLimit: Int) {
    this()
    if (!seqVerify.matcher(seq).matches) throw new Exception("Error in the sequence " + seq)
    trainseq = seq
    this.src2tgt = src2tgt
    this.rootDir = generalRoot
    this.memoryLimit = memoryLimit
  }

  def main(args: Array[String]) {
    val seq: String = "PH*H*H*H*H*3*3*3*4*4*4*1"
    if (seqVerify.matcher(seq).matches) {
      System.err.println("OK")
    }
    else {
      System.err.println("Wrong")
    }
  }


  def runAlignment(task:GIZAAlignmentTask, step: Int, seq: String, usingSeedModel: Boolean, reducers: Int, dataSplitNum: Int,
                   prevFinalModel: Char, isFinal: Boolean, resume: Boolean): util.ArrayList[String] = {
    var model1Iterations: Int = 0
    var hmmIterations: Int = 0
    var model3Iterations: Int = 0
    var model4Iterations: Int = 0
    var i: Int = 0
    while (i < seq.length) {
          seq.charAt(i) match {
            case '1' =>
              model1Iterations += 1
            case 'H' =>
              hmmIterations += 1
            case '3' =>
              model3Iterations += 1
            case '4' =>
              model4Iterations += 1
          }
          i += 1
    }
    // why?
    if (step == 0 && model1Iterations <= 1) {
      model1Iterations = 2
    }
    var prevRoot: String = null
    if ((usingSeedModel && step == 0) || step > 0) {
      prevRoot = getTrainingRoot(this.src2tgt, step - 1, this.rootDir)
    }
    var paras: util.Map[String, String] = null

    if (this.gizaParas == null || this.gizaParas.length <= step)
      paras = defaultGIZAPara
    else {
      paras = new util.TreeMap[String, String]
      if (defaultGIZAPara != null) paras.putAll(defaultGIZAPara)
      paras.putAll(this.gizaParas(step))
    }
    val currentRoot: String = getTrainingRoot(this.src2tgt, step, this.rootDir)
   // task = new GIZAAlignmentTask(prevFinalModel, model1Iterations, hmmIterations, model3Iterations, model4Iterations, currentRoot, paras)
    logger.info("\nprevFinalModel " + prevFinalModel + "\t currentRoot " + currentRoot + "\t step " + step + "\t isFinal " + isFinal)
    logger.info("\nmodel1Iterations " + model1Iterations + "\tHMMIterations " + hmmIterations + "\tmodel3Iterations " + model3Iterations
    + "\t model4Iterations " + model4Iterations)

    task.setStartTraining(prevFinalModel)
    task.setModel1Iterations(model1Iterations)
    task.setHmmIterations(hmmIterations)
    task.setModel3Iterations(model3Iterations)
    task.setModel4Iterations(model4Iterations)
    task.setCurrentRoot(currentRoot)
    task.setPara(paras)

    task.setGeneralRoot(this.rootDir)
    //task.setCurrentRoot(currentRoot)
    task.setPrevRoot(prevRoot)
    task.setCurrStep(step)
    task.setFinal(isFinal)
    val taskStrList: util.ArrayList[String] = task.writeAlignmentTaskFile(1, dataSplitNum, this.src2tgt)
    taskStrList
  }


  def runTraining(reducer : Int, dataSplitNum : Int, nCPUs : Int, sc: SparkSession) {
    var usingSeedModel: Boolean = false
    var seq: String = null
    if (trainseq.charAt(0) == 'P') {
      usingSeedModel = true
      seq = trainseq.substring(1)
    } else {
      seq = trainseq
    }

    var nPart: Array[String] = seq.split("\\*")
    val ls: util.LinkedList[String] = new util.LinkedList[String]
    for (s <- nPart) if (s.length > 0) ls.add(s)
    logger.info("We will run these alignment training model")
    println("We will run these alignment training model\n")
    nPart = new Array[String](ls.size)
    var i: Int = 0
    for (s <- ls) {
      nPart(i) = s
      logger.info("********** " + i +" : " + s + " **********")
      println("********** " + i +" : " + s + " **********")
      i += 1
    }
    i = 0
    while (i < nPart.length) {
      var taskList = new util.ArrayList[String]()
      val task = new GIZAAlignmentTask(nCPUs)

      logger.info("Running alignment " + i )
      println("\n\t<" + i + ">.\tRunning alignment part\t" + nPart(i))
      val t0 = System.nanoTime()
      if (i == 0) {
        taskList = this.runAlignment(task, i, nPart(i), usingSeedModel, reducer, dataSplitNum, '1',
          i == nPart.length - 1, resumeIfPossible)
      }
      else {
        taskList = this.runAlignment(task, i, nPart(i), usingSeedModel, reducer, dataSplitNum, nPart(i - 1).charAt(nPart(i - 1).length - 1),
          i == nPart.length - 1, resumeIfPossible)
      }


      //1. 调用 mgiza++

      //use DatSet not RDD
/*      sc.createDataset[String](taskList)(Encoders.STRING).repartition(taskList.size())
        .foreachPartition(part => {
        val codec = new BinaryToStringCodec(false)
        part.foreach(elem => {
          val entrySplit = elem.split("\t")
          val key = entrySplit(0)
          val value = entrySplit(1)
          val cmd : CommandSheet = codec.decodeObject(value).asInstanceOf[CommandSheet]
          val message: util.LinkedList[String] = new util.LinkedList[String]
          var isSuccess : Boolean = false
          logger.info(i + "\t\tstart CAll MGIZA++ ")
          isSuccess = ExternalUtils.runCommand(cmd, key, message)
          if (isSuccess)
            logger.info(i + "\tCall MGIZA++ Sucesses")
          else {
            val bf = new StringBuilder
            bf.append("Call MGIZA++ ")
            for (elem <- message)
              bf.append(elem).append("********* #NL# *****************Failed")
            logger.info(bf.toString())
          }
        })
      })*/

      val len = taskList.size()
      sc.sparkContext.parallelize(taskList, len)
        .mapPartitionsWithIndex((j, iter) => iter.map(t => (j, t)))
        .partitionBy(new HashPartitioner(len))
        .foreachPartition(part => {
        val codec = new BinaryToStringCodec(false)
        part.foreach(elem => {
          val entrySplit = elem._2.split("\t")
          //val entrySplit = elem.split("\t")
          val key = entrySplit(0)
          val value = entrySplit(1)
          val cmd : CommandSheet = codec.decodeObject(value).asInstanceOf[CommandSheet]
          val message: util.LinkedList[String] = new util.LinkedList[String]
          var isSuccess : Boolean = false
          logger.info(i + "\t\tstart CAll MGIZA++")
          isSuccess = ExternalUtils.runCommand(cmd, key, message)
          if (isSuccess)
            logger.info(i + "\t\tCall MGIZA++ Sucesses")
          else {
            val bf = new StringBuilder(64)
            bf.append("Call MGIZA++ ")
            for (elem <- message)
              bf.append(elem).append("********* #NL# *****************Failed")
            logger.info(bf.toString())
          }
        })
      })

      val t1 = System.nanoTime()
      val T0 = TimeUnit.MILLISECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS) / 60000.0
      println("\t\tDone alignment part training task\t" + nPart(i) + "\twaste time " + T0 + " .min ")

      //2. 正则化
      logger.info("\t\t<Normalize Task : " + i + ">")
      println("\t\t<Normalize Task : " + i + ">")

      new NormalizeTask(task).normalize(dataSplitNum, reducer, this.src2tgt, sc)

      val T1 = TimeUnit.MILLISECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS) / 1000.0

      println("\t\tDone alignment part normalize task\t" + nPart(i) + "\twaste time " + T1 + " .s \n")
      i += 1
    }
  }

  def getTrainingRoot(isS2T: Boolean, step: Int, rootDir: String): String = {
    if (step >= 0) rootDir + "/training/" + (if (isS2T) "S2T" else "T2S") + "/step" + step
    else rootDir + "/training/" + (if (isS2T) "S2T" else "T2S") + "/seedmodel"
  }

  def setGizaParas(paras: util.Map[String, String]) {
    defaultGIZAPara = paras
  }
}
