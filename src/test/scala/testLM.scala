import java.io.File

import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.mt.LanguageModel.spark.exec.{GSBMain, MKNMain, KNMain, GTMain}
import edu.nju.pasalab.mt.LanguageModel.util.{NGram, Indexing}
import edu.nju.pasalab.util.configRead
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by YWJ on 2017.5.7.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object testLM {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\workspace\\winutils")

    val configFile = "data/training.conf"
    val config = ConfigFactory.parseFile(new File(configFile))

    val ep = configRead.getTrainingSettings(config)
   // Logger.getLogger("org").setLevel(Level.OFF)
   // Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer","128k")
      .config("spark.kryo.registrator", "edu.nju.pasalab.util.MyRegistrator")
      .config("spark.kryo.registrationRequired", ep.registrationRequired)
      .config("spark.default.parallelism", ep.parallelism)
      .config("spark.memory.offHeap.enabled",ep.isOffHeap)
      .config("spark.memory.offHeap.size", ep.offHeapSize)
      .config("spark.driver.extraJavaOptions", ep.driverExtraJavaOptions)
      .config("spark.executor.extraJavaOptions", ep.executorExtraJavaOptions)
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "true")
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.codec", ep.compressType)
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    configRead.printTrainingSetting(ep)
    configRead.printIntoLog(ep)

     /*val input = args(0)
    val partNum = args(1).toInt
    val N = args(2).toInt
    val splitDataRation = args(3).toDouble
    val sampleNum =  args(4).toInt
    val expectedErrorRate = args(5).toDouble
    val offset = args(6).toInt*/

    val data = NGram.encodeString(spark,
      spark.sparkContext.textFile(ep.lmInputFile, ep.partitionNum).filter(x => x.length > 0),
      ep.lmRootDir)
      /*.map(x => {
      val sp = new StringBuilder(200)
      sp.append("<s> ").append(x).append(" </s>")
      sp.toString()
    })*/

    val grams = NGram.getGrams(data, ep.N).map(_.persist(StorageLevel.MEMORY_ONLY))
    val count = NGram.getCount(grams, ep.N)
    for (elem <- count)
      print(elem + "\t")
    println()

    /*val split = data.randomSplit(Array(ep.splitDataRation, 1.0 - ep.splitDataRation), seed = 1000L)
    val (trainData, testData) = (split(0), split(1))
    val testSet = spark.sparkContext.makeRDD(testData.takeSample(false, ep.sampleNum))
    println(trainData.count() + "\n" + testSet.count())
    val indexing = Indexing.createIndex(data, ep.N)
    indexing.foreach(_.persist())*/

    if (ep.GT) GTMain.run(spark, grams, ep)

    if (ep.GSB) GSBMain.run(spark, grams, count, ep)

    if (ep.KN) KNMain.run(spark, grams, count, ep)

    if (ep.MKN) MKNMain.run(spark, grams, count, ep)

    grams.map(_.unpersist())
    spark.stop()

  }
}
