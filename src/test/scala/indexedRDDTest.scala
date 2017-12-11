import java.io.File
import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.util.configRead
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
/**
  * Created by YWJ on 2017.4.30.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object indexedRDDTest {
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

    import org.apache.spark.serializer.KryoSerializer
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

    val indexed_phrase = IndexedRDD(phrase_table)

    spark.stop()
  }
}
