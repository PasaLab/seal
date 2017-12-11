import java.io.{OutputStreamWriter, BufferedWriter, PrintWriter, File}

import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.mt.extraction.dataStructure.{SpecialString, Splitter}
import edu.nju.pasalab.mt.util.CommonFileOperations
import edu.nju.pasalab.util.configRead
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by YWJ on 2017.4.22.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object reorder {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\workspace\\winutils")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val configFile = "data/training.conf"
    val config = ConfigFactory.parseFile(new File(configFile))

    val ep = configRead.getTrainingSettings(config)

    val spark = SparkSession.builder().master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer","128k")
      .config("spark.kryo.registrator", "edu.nju.pasalab.util.MyRegistrator")
      .config("spark.kryo.registrationRequired", ep.registrationRequired)
      .config("spark.default.parallelism", ep.parallelism)
      .config("spark.driver.extraJavaOptions", ep.driverExtraJavaOptions)
      .config("spark.executor.extraJavaOptions", ep.executorExtraJavaOptions)
      .config("Direction", "C2E")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "true")
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    configRead.printTrainingSetting(ep)
    configRead.printIntoLog(ep)
    val phrase_table = spark.sparkContext.textFile(ep.rootDir + "/C2E").map(x => {
      val sp = x.split(" \\|\\|\\| ")
      val key = new StringBuilder(128)
      key.append(sp(0)).append(" ||| ").append(sp(1))
      val value = new StringBuilder(128)
      value.append(sp(2)).append(" ||| ").append(sp(3))
      (key.toString(), value.toString())
    })

    val reorder_table = spark.sparkContext.textFile(ep.rootDir + "/reorderC2E").map(x => {
      val sp = x.split(" \\|\\|\\| ")
      val key = new StringBuilder(128)
      key.append(sp(0)).append(" ||| ").append(sp(1))
      (key.toString(), sp(2))
    })


    println(phrase_table.first() + "\n" + reorder_table.first() + "\n")
    //println(phrase_table.count() + "\n" + reorder_table.count())
    CommonFileOperations.deleteIfExists(ep.rootDir + "/mergedOrder")
    val join = phrase_table.join(reorder_table)
    join.mapPartitions(part => {
      part.map(elem => {
        val key = new StringBuilder(128)
        key.append(elem._1).append(" ||| ")
        //.append(elem._2._1).append(" ||| ").append(elem._2._2)
        //val content = elem._2._1.split(" \\|\\|\\| ")(0)
        key.append(elem._2._1.split(" \\|\\|\\| ")(0)).append(" ").append(elem._2._2)
        key.toString()
      })
    })
    println(join.first())
    join.saveAsTextFile(ep.rootDir + "/mergedOrder")

    CommonFileOperations.deleteIfExists(ep.rootDir + "/merged.phrase.C2E")
    CommonFileOperations.deleteIfExists(ep.rootDir + "/merged.reorder.C2E")

    val file1 : PrintWriter = new PrintWriter(new BufferedWriter(
      new OutputStreamWriter(CommonFileOperations.openFileForWrite(ep.rootDir + "/merged.phrase.C2E", true)), 129 * 1024 * 1024))
    val file2 : PrintWriter = new PrintWriter(new BufferedWriter(
      new OutputStreamWriter(CommonFileOperations.openFileForWrite(ep.rootDir + "/sorted.reorder.C2E", true)), 129 * 1024 * 1024))
    for (elem <- join.collect()) {
      val value1 = new StringBuilder(256)
      val value2 = new StringBuilder(256)

      value1.append(elem._1).append(" ||| ")
      val content = elem._2._1.split(" \\|\\|\\| ")
      val value = content(0).trim.split("\\s+")
      value1.append(value(2)).append(" ").append(value(3)).append(" ")
        .append(value(0)).append(" ").append(value(1))
        .append(" ||| ").append(content(1).trim)

      file1.println(value1.toString())

      value2.append(elem._1).append(" ||| ").append(elem._2._2)
      file2.println(value2.toString())
    }

    file1.close()
    file2.close()
    spark.stop()
  }
}
