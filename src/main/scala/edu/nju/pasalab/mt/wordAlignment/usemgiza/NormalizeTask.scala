package edu.nju.pasalab.mt.wordAlignment.usemgiza

import java.io.IOException
import java.util

import chaski.utils.CommandSheet.Command
import chaski.utils.{CommandSheet, BinaryToStringCodec}
import edu.nju.pasalab.mt.util.{SntToCooc, CommonFileOperations}
import edu.nju.pasalab.mt.wordAlignment.util.{GIZAAlignmentTask, ExternalUtils}
import edu.nju.pasalab.util.tupleEncoders
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
/**
  * Model 1: TTable means word translation table
  * HMM Model:
  *          TTable means word translation table
  *          ATable means alignment table
  *          HMMTable HMM skip table
  * Model 3/4:
  *          TTable means word translation table
  *          ATable means alignment table
  *          NTable means fertility table(繁衍度表)
  *          DTable means distribution table
  *          ps     means NULL word probability
  * Model 4:
  *         D4Table means distribution table for IBM model 4
  *
  * Created by YWJ at 2016/6/21
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
class NormalizeTask(task: GIZAAlignmentTask) extends Serializable with tupleEncoders{
  //

  val logger = LoggerFactory.getLogger(classOf[NormalizeTask])

  def normalize(dataSplitNum: Int, totalReducer:Int, src2tgt: Boolean, sc: SparkSession) {

    val tInput: String = GIZAAlignmentTask.getModelDir(task.getCurrentRoot, 't', true)
    val tOutput: String = GIZAAlignmentTask.getModelDir(task.getCurrentRoot, 't', false)
    CommonFileOperations.deleteIfExists(tOutput)
    normalizeTTable(tInput, tOutput, totalReducer, sc)

    // normalize ADTable
    if (task.getHmmIterations > 0 || task.getModel3Iterations > 0 || task.getModel4Iterations > 0) {
      val aInput: String = GIZAAlignmentTask.getModelDir(task.getCurrentRoot, 'a', true)
      val aOutput: String = GIZAAlignmentTask.getModelDir(task.getCurrentRoot, 'a', false)
      CommonFileOperations.deleteIfExists(aOutput)
      normalizeADTable(aInput, aOutput, totalReducer, sc)
    }

    if (task.getModel3Iterations > 0 || task.getModel4Iterations > 0) {
      val dInput: String = GIZAAlignmentTask.getModelDir(task.getCurrentRoot, 'd', true)
      val dOutput: String = GIZAAlignmentTask.getModelDir(task.getCurrentRoot, 'd', false)
      CommonFileOperations.deleteIfExists(dOutput)
      normalizeADTable(dInput, dOutput, totalReducer, sc)

      val nInput: String = GIZAAlignmentTask.getModelDir(task.getCurrentRoot, 'n', true)
      //val nTemp: String = GIZAAlignmentTask.getDTableTemp(task.getCurrentRoot)
      val nOutput: String = GIZAAlignmentTask.getModelDir(task.getCurrentRoot, 'n', false)
      CommonFileOperations.deleteIfExists(nOutput)
      normalizeNTable(nInput, nOutput, totalReducer, sc)
    }
    // If HMM is the last step, we need to normalize HMM Table
    if (task.getHmmIterations > 0 && (task.getModel3Iterations == 0 && task.getModel4Iterations == 0)) {
      normalizeHTable(src2tgt, dataSplitNum, sc)
      //jobs.add(job_HNorm)

    }
    // If Model 4 is the last step, we need to normalize D4 Model
    if (task.getModel4Iterations > 0) {
      normalizeD4Table(src2tgt, dataSplitNum, sc)
    }
    //jobs.submit()
    //jobs.waitForCompletion(false)
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  @throws(classOf[ClassNotFoundException])
  private def normalizeD4Table(src2tgt: Boolean, dataSplitNum: Int, sc: SparkSession) {
    val cmds: chaski.utils.CommandSheet = new CommandSheet
    val cmd: Command = new Command
    cmd.setExecutable(GIZAAlignmentTask.d4normbinary)
    cmd.setExternal(true)
    cmd.setRedirStdout("d4norm.log")
    val args: util.List[String] = new util.LinkedList[String]
    if (src2tgt) {
      cmds.addInitFile(SntToCooc.getHDFSSrcVCBDir(task.getGeneralRoot), GIZAAlignmentTask.localSrcVcb, true)
      cmds.addInitFile(SntToCooc.getHDFSTgtVCBDir(task.getGeneralRoot), GIZAAlignmentTask.localTgtVcb, true)
      cmds.addInitFile(SntToCooc.getHDFSSrcVCBClassPath(task.getGeneralRoot), GIZAAlignmentTask.localSrcVcbClass, true)
      cmds.addInitFile(SntToCooc.getHDFSTgtVCBClassPath(task.getGeneralRoot), GIZAAlignmentTask.localTgtVcbClass, true)
    }
    else {
      cmds.addInitFile(SntToCooc.getHDFSTgtVCBDir(task.getGeneralRoot), GIZAAlignmentTask.localSrcVcb, true)
      cmds.addInitFile(SntToCooc.getHDFSSrcVCBDir(task.getGeneralRoot), GIZAAlignmentTask.localTgtVcb, true)
      cmds.addInitFile(SntToCooc.getHDFSTgtVCBClassPath(task.getGeneralRoot), GIZAAlignmentTask.localSrcVcbClass, true)
      cmds.addInitFile(SntToCooc.getHDFSSrcVCBClassPath(task.getGeneralRoot), GIZAAlignmentTask.localTgtVcbClass, true)
    }
    args.add(GIZAAlignmentTask.localSrcVcb)
    args.add(GIZAAlignmentTask.localTgtVcb)
    val d4model: String = GIZAAlignmentTask.getModelPath(-1, '4', task.getCurrentRoot, false)

    cmds.addPostFile("tmp/d4model", d4model)
    cmds.addPostFile("tmp/d4model.b", d4model + ".b")
    CommonFileOperations.deleteIfExists(d4model)
    CommonFileOperations.deleteIfExists(d4model + ".b")
    args.add("tmp/d4model")
    val parts: Int = dataSplitNum

    var j: Int = 1
    while (j <= parts) {

      val countPath: String = GIZAAlignmentTask.getModelPath(j, '4', task.getCurrentRoot, true)
      cmds.addInitFile(countPath, "tmp/d4model.count." + j, false)
      cmds.addInitFile(countPath + ".b", "tmp/d4model.count." + j + ".b", false)
      args.add("tmp/d4model.count." + j)
      j += 1

    }
    cmds.setMissingFileThreshold(parts / 10)
    val paras: Array[String] = new Array[String](args.size)
    var i: Int = 0
    for (a <- args) {
      paras(i) = a
      i += 1
    }
    cmd.setArguments(paras)
    val cList: util.List[Command] = new util.LinkedList[Command]
    cList.add(cmd)
    cmds.setCommands(cList)

    val codec : BinaryToStringCodec = new BinaryToStringCodec(false)
    val tasks = Array(codec.encodeObject(cmds))

    //TODO use different ways
    logger.info("\n********************* start IBM Model4 D4Table normalize********************\n")

    sc.sparkContext.parallelize(tasks)
      .mapPartitionsWithIndex((i, part) => part.map(t => (i,t)))
      .partitionBy(new HashPartitioner(tasks.length))
      .foreachPartition(part => {
        val codec = new BinaryToStringCodec(false)
        part.foreach(t => {
          var retry = 1
          logger.info("\n***************************************" + t._2 + " ***************************************\n")
          var test = t._2
          var cmd : CommandSheet = null
          cmd = codec.decodeObject(test).asInstanceOf[CommandSheet]
          val message: util.LinkedList[String] = new util.LinkedList[String]
          var isSuccess : Boolean = false
          logger.info("**** start IBM Model4 D4Table normalize******************************")
          test = t._2
          isSuccess = ExternalUtils.runCommand(cmd, test, message)
          if (isSuccess)
            logger.info("IBM Model4 D4Table normalize Sucesses")
          else if(retry < 100 && !isSuccess) {
            while (!isSuccess && retry < 100) {
              logger.info("\n\nRetry again " + retry)
              message.clear()
              test = t._2
              cmd = codec.decodeObject(test).asInstanceOf[CommandSheet]
              isSuccess = ExternalUtils.runCommand(cmd, test, message)
              retry += 1
            }
            if (!isSuccess) {
              val bf = new StringBuilder(64)
              bf.append("Call MGIZA++ to D4Table normalize")
              for (elem <- message)
                bf.append(elem).append("********* #NL# *****************Failed")
              logger.info(bf.toString())
              throw new IOException(bf.toString())
            }
          } else {
            val bf = new StringBuilder(64)
            bf.append("Call MGIZA++ to D4Table normalize")
            for (elem <- message)
              bf.append(elem).append("********* #NL# *****************Failed")
            logger.info(bf.toString())
            throw new IOException(bf.toString())
          }
        })
      })
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  @throws(classOf[ClassNotFoundException])
  private def normalizeHTable(src2tgt: Boolean, dataSplitNum: Int, sc: SparkSession) {
    val cmds: chaski.utils.CommandSheet = new CommandSheet()
    val cmd: CommandSheet.Command = new CommandSheet.Command()
    cmd.setExecutable(GIZAAlignmentTask.hmmnormbinaray)
    cmd.setExternal(true)
    cmd.setRedirStdout("hmmnorm.log")
    //val args: util.List[String] = new util.LinkedList[String]
    val args :util.LinkedList[String] = new util.LinkedList[String]()
    if (src2tgt) {
      cmds.addInitFile(SntToCooc.getHDFSSrcVCBDir(task.getGeneralRoot), GIZAAlignmentTask.localSrcVcb, true)
      cmds.addInitFile(SntToCooc.getHDFSTgtVCBDir(task.getGeneralRoot), GIZAAlignmentTask.localTgtVcb, true)
      cmds.addInitFile(SntToCooc.getHDFSSrcVCBClassPath(task.getGeneralRoot), GIZAAlignmentTask.localSrcVcbClass, true)
      cmds.addInitFile(SntToCooc.getHDFSTgtVCBClassPath(task.getGeneralRoot), GIZAAlignmentTask.localTgtVcbClass, true)
    }
    else {
      cmds.addInitFile(SntToCooc.getHDFSTgtVCBDir(task.getGeneralRoot), GIZAAlignmentTask.localSrcVcb, true)
      cmds.addInitFile(SntToCooc.getHDFSSrcVCBDir(task.getGeneralRoot), GIZAAlignmentTask.localTgtVcb, true)
      cmds.addInitFile(SntToCooc.getHDFSTgtVCBClassPath(task.getGeneralRoot), GIZAAlignmentTask.localSrcVcbClass, true)
      cmds.addInitFile(SntToCooc.getHDFSSrcVCBClassPath(task.getGeneralRoot), GIZAAlignmentTask.localTgtVcbClass, true)
    }
    args.add(GIZAAlignmentTask.localSrcVcb)
    args.add(GIZAAlignmentTask.localTgtVcb)

    val hmmOutput: String = GIZAAlignmentTask.getModelPath(-1, 'h', task.getCurrentRoot, false)
    cmds.addPostFile("tmp/hmmmodel", hmmOutput)
    cmds.addPostFile("tmp/hmmmodel.alpha", hmmOutput + ".alpha")
    cmds.addPostFile("tmp/hmmmodel.beta", hmmOutput + ".beta")
    CommonFileOperations.deleteIfExists(hmmOutput)
    CommonFileOperations.deleteIfExists(hmmOutput + ".alpha")
    CommonFileOperations.deleteIfExists(hmmOutput + ".beta")
    args.add("tmp/hmmmodel")
    val parts: Int = dataSplitNum
    var j: Int = 1
    while (j <= parts) {
      val countPath: String = GIZAAlignmentTask.getModelPath(j, 'h', task.getCurrentRoot, true)
      cmds.addInitFile(countPath, "tmp/hmmmodel.count." + j, false)
      cmds.addInitFile(countPath + ".alpha", "tmp/hmmmodel.count." + j + ".alpha", false)
      cmds.addInitFile(countPath + ".beta", "tmp/hmmmodel.count." + j + ".beta", false)
      args.add("tmp/hmmmodel.count." + j)
      j += 1
    }

    cmds.setMissingFileThreshold(parts / 10)
    val paras: Array[String] = new Array[String](args.size)
    var i: Int = 0
    import scala.collection.JavaConversions._
    for (a <- args) {
      paras(i) = a
      i += 1
    }
    cmd.setArguments(paras)
    val cList: util.List[Command] = new util.LinkedList[Command]
    cList.add(cmd)
    cmds.setCommands(cList)

    val codec : BinaryToStringCodec = new BinaryToStringCodec(false)
    val tasks = Array(codec.encodeObject(cmds))

    //TODO use different ways
    logger.info("\n********************* start HMM  normalizeHTable********************\n")
    sc.sparkContext.parallelize(tasks)
      .mapPartitionsWithIndex((i, iter) => iter.map(t => (i,t)))
      .partitionBy(new HashPartitioner(tasks.length))
      .foreachPartition(iter => {
        val codec = new BinaryToStringCodec(false)
        iter.foreach(t => {
          logger.info("\n***************************************" + t._2 + " ***************************************\n")
          var test = t._2
          var retry = 1
          val cmd : CommandSheet = codec.decodeObject(test).asInstanceOf[CommandSheet]
          val message: util.LinkedList[String] = new util.LinkedList[String]
          var isSuccess : Boolean = false
          logger.info("**** start HMM  normalizeHTable******************************")
          isSuccess = ExternalUtils.runCommand(cmd, test, message)
          if (isSuccess)
            logger.info("HMM  normalizeHTable Sucesses")
          else if (retry < 200 && !isSuccess) {
            while (retry < 200 && !isSuccess) {
              logger.info("\n\nRetry again " + retry)
              message.clear()
              test = t._2
              isSuccess = ExternalUtils.runCommand(cmd, test, message)
              retry += 1
            }
            if (!isSuccess) {
              val bf = new StringBuilder(64)
              bf.append("Call MGIZA++ to HMM  normalizeHTable")
              for (elem <- message)
                bf.append(elem).append("********* #NL# *****************Failed")
              logger.info(bf.toString())
              throw new IOException(bf.toString())
            }
          } else {
            val bf = new StringBuilder(64)
            bf.append("Call MGIZA++ to HMM normalizeHTable")
            for (elem <- message)
              bf.append(elem).append("********* #NL# *****************Failed")
            logger.info(bf.toString())
          }
        })
      })
    //false
  }

  private def normalizeNTable(input: String, output: String, totalReducer: Int, sc: SparkSession) = {
    val probThreshold = 1e-7

    val counts = sc.sparkContext.textFile(input,totalReducer).map(v => {
      val pa: Array[String] = v.split("\\s+", 2)
      (pa(0), pa(1))
    }).reduceByKey((v1, v2) => {
      val p1 = v1.split("\\s+").map(_.toDouble)
      val p2 = v2.split("\\s+").map(_.toDouble)
      val p1Size = p1.length
      val p2Size = p2.length

      val len = if (p1Size > p2Size) p1Size else p2Size

      val resultArr = new Array[Double](len)
      for (i <- 0 until p1Size)
        resultArr(i) += p1(i)
      for (i <- 0 until p2Size)
        resultArr(i) += p2(i)

      resultArr.mkString(" ")
    }).map(t => {
      val sp = t._2.split("\\s+")
      val pt : DoubleArrayList = new DoubleArrayList(sp.size)
      var sumValue : Double = 0
      for (i <- 0 until sp.size) {
        val t = sp(i).toDouble
        pt.add(t)
        sumValue += t
      }

      val bf = new StringBuffer()
      bf.append(t._1)
      for (i <- 0 until pt.size) {
        bf.append(" ")
        val value = pt(i) / sumValue
        if (value < probThreshold)
          bf.append(probThreshold)
        else
          bf.append(value)
      }
      bf.toString
    })

    counts.saveAsTextFile(output)

    //TODO 此处与chaski的逻辑不一样，因为怀疑chaski写错了
  }

  private def normalizeTTable(input: String, output: String, totalReducer: Int, sc: SparkSession) = {
    CommonFileOperations.deleteIfExists(output)
    val probThreshold = 1e-7

    /*implicit val myObjEncoder1 = org.apache.spark.sql.Encoders.kryo[(String, String)]
    implicit val myObjEncoder2 = org.apache.spark.sql.Encoders.kryo[(String, Double)]
    implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)*/

   /* import sc.implicits._
    val count = sc.read.textFile(input).mapPartitions(part => {
      val countThreshold = -1
      part.flatMap(v => {
        val resultList = new util.ArrayList[(String, Double)]()
        val pa: Array[String] = v.split("\\s+", 3)
        val iv = pa(0).toInt
        if (countThreshold <= 0 || countThreshold >= 1)
          resultList.add((iv + " " + pa(1), pa(2).toDouble))
        else {
          if ( pa(2).toDouble > countThreshold)
            resultList.add((iv + " " + pa(1), pa(2).toDouble))
        }
        resultList.toArray[(String, Double)](new Array[(String, Double)](resultList.size()))
      })
    }).groupByKey(elem => elem._1).mapGroups((k, v) => {
      var sum = 0.0
      v.foreach(elem => {
        sum += elem._2
      })
      (k, sum)
    })

    val edgeCount = count.map(t =>(t._1.trim.split("\\s+")(0), t._2)).groupByKey(elem => elem._1).mapGroups((k, v) => {
      var sum = 0.0
      v.foreach(elem => {
        sum += elem._2
      })
      (k, sum)
    }).toDF("_1","_2").as[(String, Double)].alias("edgeCountDS")


    val countA = count.map(elem => {
      val sp = elem._1.trim.split("\\s+")
      (sp(0), sp(1) + " " + elem._2)}).toDF("_1","_2").as[(String,String)].alias("countDS")

/*  val partitioned2 = edgeCount.repartition(edgeCount("_1")).sortWithinPartitions(edgeCount("_1")).as[(String, Double)].alias("edgeCountDS").cache()
    val partitioned1 = countA.toDF("_1","_2").repartition(countA("_1")).sortWithinPartitions(countA("_1")).as[(String,String)].alias("countDS").cache()
    partitioned1.show(1)
    partitioned2.show(1)*/

    val update = countA.joinWith(edgeCount, $"countDS._1" === $"edgeCountDS._1", "inner").map(elem => {
      val pa = elem._1._2.trim.split("\\s+")
      (elem._1._1 + " " + pa(0), pa(1).toDouble / elem._2._2)
    }).filter(_._2 > probThreshold).map(x => x._1 + " " + x._2).rdd
    update.saveAsTextFile(output)*/


   val counts = sc.sparkContext.textFile(input, totalReducer).mapPartitions(iter => {
      val countThreshold = -1
      iter.flatMap(v => {
        val resultList = new util.ArrayList[(String, Double)]()
        val pa: Array[String] = v.split("\\s+", 3)
        val iv = pa(0).toInt
        if (countThreshold <= 0 || countThreshold >= 1)
          resultList.add((iv + " " + pa(1), pa(2).toDouble))
        else {
          if ( pa(2).toDouble > countThreshold) {
            resultList.add((iv + " " + pa(1), pa(2).toDouble))
          }
        }
        resultList.toArray[(String, Double)](new Array[(String, Double)](resultList.size()))
      })
    }).reduceByKey(_+_)
    counts.persist(StorageLevel.MEMORY_ONLY)
    println(counts.count())

    val edgeCounts = counts.map(t => (t._1.trim.split("\\s+")(0), t._2)).reduceByKey(_+_)

    val updatedParas = counts.map(elem => {
      val sp = elem._1.trim.split("\\s+")
      (sp(0), sp(1) + " " + elem._2)
    })
      .join(edgeCounts).map(elem => {
      val pa = elem._2._1.trim.split("\\s+")
      (elem._1 + " " + pa(0), pa(1).toDouble / elem._2._2)
    }).filter(_._2 > probThreshold).map(t => t._1 + " " + t._2)

    updatedParas.saveAsTextFile(output)
    counts.unpersist()
  }

  private def normalizeADTable(input: String, output: String, totalReducer: Int, sc: SparkSession) = {

    val probThreshold = 1e-7

   /* import sc.implicits._
    val count = sc.read.textFile(input).map(v => {
      val pa = v.split("\\s+", 5)
      (pa(1) + " " + pa(2) + " " + pa(3) + "|" + pa(0), pa(4).toDouble)
    }).groupByKey(elem => elem._1).mapGroups((k, v) => {
      var sum = 0.0
      v.foreach(elem => {
        sum += elem._2
      })
      (k, sum)
    })

    val edgeCount = count.map(t => (t._1.split("\\|")(0), t._2)).groupByKey(elem =>elem._1).mapGroups((k, v) => {
      var sum = 0.0
      v.foreach(elem => {
        sum += elem._2
      })
      (k, sum)
    }).toDF("_1","_2").as[(String, Double)].alias("edgeCountDS")

    val tmp = count.map(t => (t._1.split("\\|")(0), t._1.split("\\|")(1) + " " + t._2))
      .toDF("_1", "_2").as[(String,String)].alias("countDS")

    val update = tmp.joinWith(edgeCount, $"countDS._1" === $"edgeCountDS._1", "inner").map(elem => {
      val pa = elem._1._2.trim.split("\\s+")
      (pa(0) + " " + elem._1._1, pa(1).toDouble/elem._2._2)
    }).filter(_._2 > probThreshold).map(x => x._1 + " " + x._2)

    update.rdd.saveAsTextFile(output)*/

    val counts = sc.sparkContext.textFile(input,totalReducer).map(v => {
      val pa = v.split("\\s+", 5)
      val key: String = pa(1) + " " + pa(2) + " " + pa(3) + "|" + pa(0)
      val value = pa(4).toDouble
      (key, value)
    }).reduceByKey(_ + _)
    counts.persist(StorageLevel.MEMORY_ONLY)
    println(counts.count())

    val edgeCounts = counts
      .map(t => (t._1.split("\\|")(0), t._2))
      .reduceByKey(_ + _)

    val updatedParas = counts.map(t => (t._1.split("\\|")(0), t._1.split("\\|")(1) + " " + t._2))
      .join(edgeCounts).map(t => {
      val pa = t._2._1.split("\\s+")
      (pa(0) + " " + t._1, pa(1).toDouble / t._2._2)
    }).filter(_._2 > probThreshold).map(x => x._1 + " " + x._2)

    updatedParas.saveAsTextFile(output)
   counts.unpersist()
  }
}
